package com.alad.app.core.network

import android.util.Base64
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.MultipartBody
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.asRequestBody
import org.json.JSONObject
import java.io.File
import java.io.IOException
import java.util.concurrent.TimeUnit

/**
 * Client for the companion Hugging Face Docker Space in /huggingface-space.
 * It performs actual remote Demucs separation. It is intentionally not called
 * from the 40 ms capture callback: free CPU Spaces are too slow for that.
 * The live pipeline must buffer a clip, call separate(), then synchronize stems
 * with the translation/TTS stream before this can safely replace raw audio.
 */
class HuggingFaceSeparationClient(
    private val baseUrl: String,
    private val bearerToken: String? = null,
    private val client: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(20, TimeUnit.SECONDS)
        .readTimeout(260, TimeUnit.SECONDS)
        .writeTimeout(60, TimeUnit.SECONDS)
        .callTimeout(300, TimeUnit.SECONDS)
        .build()
) {
    data class Stems(val vocalsWav: ByteArray, val instrumentalWav: ByteArray, val model: String)

    suspend fun separate(audioFile: File): Stems = withContext(Dispatchers.IO) {
        require(audioFile.isFile && audioFile.length() > 44) { "Audio file is missing or empty" }
        val cleanBase = baseUrl.trim().trimEnd('/')
        require(cleanBase.startsWith("https://")) { "Hugging Face Space URL must use HTTPS" }
        val body = MultipartBody.Builder().setType(MultipartBody.FORM)
            .addFormDataPart("file", audioFile.name,
                audioFile.asRequestBody("application/octet-stream".toMediaType()))
            .build()
        val requestBuilder = Request.Builder().url("$cleanBase/separate").post(body)
        if (!bearerToken.isNullOrBlank()) requestBuilder.header("Authorization", "Bearer ${bearerToken.trim()}")
        client.newCall(requestBuilder.build()).execute().use { response ->
            val responseText = response.body?.string().orEmpty()
            if (!response.isSuccessful) throw IOException("Hugging Face separation HTTP ${response.code}: ${responseText.take(600)}")
            val json = JSONObject(responseText)
            if (!json.optBoolean("ok", false)) throw IOException("Separation service did not return ok=true")
            val vocals = json.optString("vocals_wav_base64")
            val instrumental = json.optString("instrumental_wav_base64")
            if (vocals.isBlank() || instrumental.isBlank()) throw IOException("Missing separated audio stems in response")
            Stems(
                vocalsWav = Base64.decode(vocals, Base64.DEFAULT),
                instrumentalWav = Base64.decode(instrumental, Base64.DEFAULT),
                model = json.optString("model", "unknown")
            )
        }
    }
}
