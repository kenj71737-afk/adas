package com.alad.app.core.audio

import android.util.Log
import com.alad.app.core.network.HuggingFaceSeparationClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Buffered live source-separation pipeline. Captures 4-second 16 kHz mono PCM
 * windows, sends each window to a real Demucs service, then routes vocals to the
 * translation WebSocket and instrumental audio to a separate Android AudioTrack.
 * A remote CPU Space is not guaranteed to keep up with real time; bounded queues
 * prevent unbounded latency and status callbacks report overload/errors.
 */
class LiveSeparationPipeline(
    private val cacheDir: File,
    spaceUrl: String,
    private val onSegmentReady: () -> Unit,
    private val onVocalsPcm: (ByteArray) -> Unit,
    private val onInstrumentalPcm: (ByteArray) -> Unit,
    private val onStatus: (String) -> Unit,
    bearerToken: String? = null
) {
    companion object {
        private const val TAG = "LiveSeparationPipeline"
        private const val SAMPLE_RATE = 16_000
        private const val SEGMENT_MS = 4_000
        private const val SEGMENT_BYTES = SAMPLE_RATE * 2 * SEGMENT_MS / 1000
        private const val INPUT_CHUNK_BYTES = 1280 // 40 ms at 16 kHz mono PCM16
    }

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val client = HuggingFaceSeparationClient(spaceUrl, bearerToken)
    private val queue = Channel<ByteArray>(capacity = 2)
    private val stopped = AtomicBoolean(false)
    private val buffer = ByteArrayOutputStream(SEGMENT_BYTES * 2)

    init {
        scope.launch {
            for (segment in queue) {
                if (stopped.get()) break
                var wavFile: File? = null
                try {
                    onStatus("AI đang tách lời thoại…")
                    wavFile = File.createTempFile("alad-live-", ".wav", cacheDir)
                    writePcmWav(segment, wavFile)
                    val stems = client.separate(wavFile)
                    val vocals = WavPcm.to16kMono(stems.vocalsWav)
                    val instrumental = WavPcm.to16kMono(stems.instrumentalWav)
                    if (vocals.isEmpty() || instrumental.isEmpty()) {
                        throw IOException("Demucs trả về âm thanh rỗng")
                    }
                    // Open a new sync window. Instrumental stem waits for the first translated audio chunk.
                    onSegmentReady()
                    // Stream vocals as small frames to the live translation WebSocket.
                    var offset = 0
                    while (offset < vocals.size && !stopped.get()) {
                        val end = minOf(offset + INPUT_CHUNK_BYTES, vocals.size)
                        onVocalsPcm(vocals.copyOfRange(offset, end))
                        offset = end
                    }
                    // Separate AudioTrack consumes this as background audio in real time.
                    var bgOffset = 0
                    val frameBytes = 4096
                    while (bgOffset < instrumental.size && !stopped.get()) {
                        val end = minOf(bgOffset + frameBytes, instrumental.size)
                        onInstrumentalPcm(instrumental.copyOfRange(bgOffset, end))
                        bgOffset = end
                    }
                    onStatus("AI tách âm thanh hoạt động · ${stems.model}")
                } catch (e: Exception) {
                    Log.e(TAG, "Live separation failed", e)
                    onStatus("Lỗi tách âm thanh: ${e.message ?: "không rõ nguyên nhân"}. Kiểm tra URL/Space Hugging Face.")
                } finally {
                    wavFile?.delete()
                }
            }
        }
    }

    @Synchronized
    fun accept(pcm16kMono: ByteArray) {
        if (stopped.get() || pcm16kMono.isEmpty()) return
        buffer.write(pcm16kMono)
        val all = buffer.toByteArray()
        var consumed = 0
        while (all.size - consumed >= SEGMENT_BYTES) {
            val segment = all.copyOfRange(consumed, consumed + SEGMENT_BYTES)
            if (queue.trySend(segment).isFailure) {
                onStatus("AI Hugging Face xử lý chậm; bỏ một đoạn để tránh tăng độ trễ.")
            }
            consumed += SEGMENT_BYTES
        }
        buffer.reset()
        if (consumed < all.size) buffer.write(all, consumed, all.size - consumed)
    }

    fun stop() {
        if (stopped.compareAndSet(false, true)) {
            queue.close()
            scope.cancel()
            synchronized(this) { buffer.reset() }
        }
    }

    private fun writePcmWav(pcm: ByteArray, target: File) {
        FileOutputStream(target).use { out ->
            out.write("RIFF".toByteArray(Charsets.US_ASCII))
            writeLe32(out, 36 + pcm.size)
            out.write("WAVEfmt ".toByteArray(Charsets.US_ASCII))
            writeLe32(out, 16)
            writeLe16(out, 1) // PCM
            writeLe16(out, 1) // mono
            writeLe32(out, SAMPLE_RATE)
            writeLe32(out, SAMPLE_RATE * 2)
            writeLe16(out, 2)
            writeLe16(out, 16)
            out.write("data".toByteArray(Charsets.US_ASCII))
            writeLe32(out, pcm.size)
            out.write(pcm)
        }
    }

    private fun writeLe16(out: FileOutputStream, n: Int) {
        out.write(n and 0xff); out.write((n shr 8) and 0xff)
    }
    private fun writeLe32(out: FileOutputStream, n: Int) {
        out.write(n and 0xff); out.write((n shr 8) and 0xff); out.write((n shr 16) and 0xff); out.write((n shr 24) and 0xff)
    }
}

/** Minimal WAV PCM16 decoder/downmixer/resampler; rejects unsupported encodings. */
private object WavPcm {
    fun to16kMono(wav: ByteArray): ByteArray {
        if (wav.size < 44 || String(wav, 0, 4, Charsets.US_ASCII) != "RIFF" || String(wav, 8, 4, Charsets.US_ASCII) != "WAVE") {
            throw IOException("Đầu ra Demucs không phải WAV hợp lệ")
        }
        var pos = 12
        var channels = 0
        var sampleRate = 0
        var bits = 0
        var format = 0
        var dataOffset = -1
        var dataSize = 0
        while (pos + 8 <= wav.size) {
            val id = String(wav, pos, 4, Charsets.US_ASCII)
            val size = le32(wav, pos + 4).coerceAtLeast(0)
            val start = pos + 8
            if (start + size > wav.size) break
            when (id) {
                "fmt " -> if (size >= 16) {
                    format = le16(wav, start); channels = le16(wav, start + 2)
                    sampleRate = le32(wav, start + 4); bits = le16(wav, start + 14)
                }
                "data" -> { dataOffset = start; dataSize = size; break }
            }
            pos = start + size + (size and 1)
        }
        if (format != 1 || bits != 16 || channels !in 1..2 || sampleRate <= 0 || dataOffset < 0) {
            throw IOException("Chỉ hỗ trợ WAV PCM 16-bit mono/stereo từ Demucs")
        }
        val frameBytes = channels * 2
        val frames = minOf(dataSize, wav.size - dataOffset) / frameBytes
        if (frames == 0) return ByteArray(0)
        val mono = ShortArray(frames)
        for (i in 0 until frames) {
            var sum = 0
            for (ch in 0 until channels) sum += le16(wav, dataOffset + i * frameBytes + ch * 2).toShort().toInt()
            mono[i] = (sum / channels).coerceIn(-32768, 32767).toShort()
        }
        val outFrames = (frames.toLong() * 16_000 / sampleRate).toInt().coerceAtLeast(1)
        val out = ByteArray(outFrames * 2)
        for (i in 0 until outFrames) {
            val src = i.toDouble() * sampleRate / 16_000.0
            val left = src.toInt().coerceIn(0, frames - 1)
            val right = (left + 1).coerceAtMost(frames - 1)
            val frac = (src - left).toFloat()
            val value = (mono[left] * (1f - frac) + mono[right] * frac).toInt().coerceIn(-32768, 32767)
            out[i * 2] = (value and 0xff).toByte()
            out[i * 2 + 1] = ((value shr 8) and 0xff).toByte()
        }
        return out
    }
    private fun le16(a: ByteArray, p: Int) = (a[p].toInt() and 255) or ((a[p + 1].toInt() and 255) shl 8)
    private fun le32(a: ByteArray, p: Int) = (a[p].toInt() and 255) or ((a[p + 1].toInt() and 255) shl 8) or ((a[p + 2].toInt() and 255) shl 16) or (a[p + 3].toInt() shl 24)
}
