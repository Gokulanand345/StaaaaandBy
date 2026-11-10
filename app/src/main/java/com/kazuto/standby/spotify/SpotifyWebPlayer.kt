package com.kazuto.standby.spotify

import android.content.Context
import android.graphics.Bitmap
import android.os.SystemClock
import android.util.Log
import com.kazuto.standby.media.ArtLoader
import com.kazuto.standby.media.NowPlaying
import com.kazuto.standby.media.TrackRef
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import org.json.JSONObject
import kotlin.math.abs

/**
 * Spotify Web API でアカウントの再生状態を読む係。
 * スマホ上の Spotify の鏡(他端末の再生の写し)は静かに切れるが、こちらは
 * どの端末で再生していても Spotify のサーバーから直接聞くので切れない。
 *
 * 自分では定期的に叩かない。いつ聞くかは [HybridPlaybackSource] が決め、
 * [refresh] を呼ぶたびに `GET /me/player` を1回だけ叩く(以前は1秒ごとに叩いていたが、
 * Spotify の上限に当たると十数時間止められるのでやめた)。聞いていない間は再生位置を
 * 外挿する。読んだ位置の基準時刻は往復の中間点にする
 * (`progress_ms` は往復のどこかで採られていて、中間なら最悪誤差が半分になる)。
 * jizura-sync の web/spotify.js の SpotifyPlayer の移植。
 */
class SpotifyWebPlayer(private val context: Context) {

    /** [refresh] の結果 */
    enum class Result { OK, RATE_LIMITED, AUTH, FAILED }

    companion object {
        private const val TAG = "StaaaaandBy"
        /** 外挿と実測のズレがこれ未満ならジッタ扱いで基準を動かさない */
        private const val RESYNC_MS = 120L
    }

    private val _nowPlaying = MutableStateFlow<NowPlaying?>(null)
    val nowPlaying: StateFlow<NowPlaying?> = _nowPlaying

    private val _upNext = MutableStateFlow<TrackRef?>(null)
    val upNext: StateFlow<TrackRef?> = _upNext

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private var isPlaying = false
    private var trackId: String? = null
    private var art: Pair<String, Bitmap?>? = null   // artUrl → bitmap(読み込み中は null)
    private var seekAt = 0L                          // 操作直後の古いポーリング結果を捨てる

    fun stop() {
        scope.cancel()
    }

    /** 再生/停止を切り替える。通ったら true(呼び出し側が少し待って [refresh] で拾い直す) */
    suspend fun playPause(): Boolean = command(if (isPlaying) "PUT" to "/me/player/pause" else "PUT" to "/me/player/play")
    suspend fun skipToNext(): Boolean = command("POST" to "/me/player/next")
    suspend fun skipToPrevious(): Boolean = command("POST" to "/me/player/previous")

    private suspend fun command(call: Pair<String, String>): Boolean =
        try {
            seekAt = SystemClock.elapsedRealtime()
            SpotifyAuth.api(context, call.first, call.second)
            true
        } catch (e: SpotifyAuth.SpotifyException) {
            Log.w(TAG, "spotify command ${call.second} failed: ${e.message}")
            false
        }

    /** `GET /me/player` を1回叩いて [nowPlaying] を更新する。 */
    suspend fun refresh(): Result {
        val t0 = SystemClock.elapsedRealtime()
        val res = try {
            SpotifyAuth.api(context, "GET", "/me/player?additional_types=track")
        } catch (e: SpotifyAuth.SpotifyException) {
            Log.w(TAG, "spotify poll failed (${e.kind}): ${e.message}")
            return when (e.kind) {
                SpotifyAuth.Kind.AUTH, SpotifyAuth.Kind.NOT_REGISTERED -> {
                    // 入り直しが要る
                    _nowPlaying.value = null
                    Result.AUTH
                }
                SpotifyAuth.Kind.RATE_LIMITED -> Result.RATE_LIMITED
                else -> Result.FAILED
            }
        }
        val t1 = SystemClock.elapsedRealtime()
        if (res.status == 204 || res.body.isBlank()) {
            // 再生中の端末が無い
            if (_nowPlaying.value != null) Log.i(TAG, "spotify: nothing is playing")
            trackId = null
            isPlaying = false
            _nowPlaying.value = null
            return Result.OK
        }
        val s = runCatching { JSONObject(res.body) }.getOrNull() ?: return Result.FAILED
        isPlaying = s.optBoolean("is_playing", false)
        val item = s.optJSONObject("item")
        if (item != null && item.optString("type") == "track" && t0 >= seekAt) {
            read(s, item, (t0 + t1) / 2)
        }
        return Result.OK
    }

    /** 曲が変わったら古い「次の曲」を捨てる */
    fun clearUpNext() {
        _upNext.value = null
    }

    /** キューの先頭(次の曲)を取って歌詞の先読みに回す。曲が変わるたびに多くて1回 */
    fun fetchQueue() {
        scope.launch {
            try {
                val res = SpotifyAuth.api(context, "GET", "/me/player/queue")
                val q = JSONObject(res.body).optJSONArray("queue")
                val item = (0 until (q?.length() ?: 0))
                    .mapNotNull { q?.optJSONObject(it) }
                    .firstOrNull { it.optString("type") == "track" }
                val ref = item?.let {
                    val artists = it.optJSONArray("artists")
                    TrackRef(
                        title = it.optString("name"),
                        artist = (0 until (artists?.length() ?: 0))
                            .mapNotNull { i -> artists?.optJSONObject(i)?.optString("name") }
                            .joinToString(", "),
                        album = it.optJSONObject("album")?.optString("name")?.ifEmpty { null },
                        durationMs = it.optLong("duration_ms", 0),
                    )
                }
                if (_upNext.value != ref) {
                    Log.i(TAG, "spotify: up next '${ref?.title}' / '${ref?.artist}'")
                    _upNext.value = ref
                }
            } catch (e: SpotifyAuth.SpotifyException) {
                Log.w(TAG, "spotify queue failed: ${e.message}")
            }
        }
    }

    private fun read(s: JSONObject, item: JSONObject, at: Long) {
        val id = item.optString("id").ifEmpty { item.optString("uri") }
        val sameTrack = id == trackId
        val playing = s.optBoolean("is_playing", false)
        val progress = s.optLong("progress_ms", 0)
        val prev = _nowPlaying.value

        if (sameTrack && prev != null && playing == prev.isPlaying && playing) {
            val predicted = prev.positionMs + (at - prev.positionUpdatedAt)
            if (abs(predicted - progress) < RESYNC_MS) return
        }

        val artists = item.optJSONArray("artists")
        val artist = (0 until (artists?.length() ?: 0))
            .mapNotNull { artists?.optJSONObject(it)?.optString("name") }
            .joinToString(", ")
        val album = item.optJSONObject("album")
        val images = album?.optJSONArray("images")
        // 画像は大きい順。最大のものをスリットスキャン用に使う
        val artUrl = images?.optJSONObject(0)?.optString("url")?.ifEmpty { null }

        if (!sameTrack) {
            trackId = id
            Log.i(TAG, "spotify: track '${item.optString("name")}' playing=$playing")
        }
        if (artUrl != null && art?.first != artUrl) {
            art = artUrl to null
            scope.launch {
                val bmp = ArtLoader.load(artUrl)
                if (art?.first == artUrl) {
                    art = artUrl to bmp
                    _nowPlaying.value?.let { if (it.albumArt == null) _nowPlaying.value = it.copy(albumArt = bmp) }
                }
            }
        }
        _nowPlaying.value = NowPlaying(
            title = item.optString("name"),
            artist = artist,
            albumArt = art?.takeIf { it.first == artUrl }?.second,
            isPlaying = playing,
            appName = "Spotify",
            durationMs = item.optLong("duration_ms", 0),
            positionMs = progress,
            positionUpdatedAt = at,
            playbackSpeed = 1f,
            album = album?.optString("name")?.ifEmpty { null },
        )
    }
}
