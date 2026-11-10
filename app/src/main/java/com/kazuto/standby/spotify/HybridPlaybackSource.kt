package com.kazuto.standby.spotify

import android.content.Context
import android.media.AudioManager
import android.os.SystemClock
import android.util.Log
import com.kazuto.standby.media.MediaSessionWatcher
import com.kazuto.standby.media.NowPlaying
import com.kazuto.standby.media.PlaybackSource
import com.kazuto.standby.media.TrackRef
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.math.abs
import kotlin.math.max
import kotlin.math.min

/**
 * Spotify につないであるときの [PlaybackSource]。端末の MediaSession([MediaSessionWatcher])を
 * 主役にし、Spotify Web API([SpotifyWebPlayer])は「答え合わせ役」にする。
 *
 * なぜ: 以前は Web API を1秒ごとに叩いていた。Spotify は上限に当たると 429 と十数時間の
 * Retry-After を返すことがあり(上限の数字は非公開)、同じアカウントを見ている他のアプリまで
 * 巻き添えになる。手元で読めるものは手元で読み、API はたまにしか呼ばない。
 *
 * 動き:
 *  - ふだんは MediaSession が主役。表示も操作も MediaSession。API は呼ばない
 *  - API に聞くのは、定期の確認([CHECK_MS])、外挿した位置が曲の終わりを過ぎたのに
 *    MediaSession が次の曲を知らせてこないとき、MediaSession が消えた/止まったとき、
 *    画面を開いたとき、だけ
 *  - API の答えと MediaSession が食い違ったら(曲・再生/停止・位置)鏡が切れているので
 *    API を主役にし、[WEB_POLL_MS] ごとに聞く。MediaSession が API と同じ状態に戻ったら
 *    主役を戻す
 *  - 429 のあいだ(再開時刻は Prefs に保存。[SpotifyAuth.api])とサインインが切れている
 *    あいだは API を呼ばず、MediaSession だけで表示を続ける
 *  - タップ操作は、そのとき主役の側に送る(MediaSession 側が「いま送ると危ない」と
 *    言っているときだけは API に送る)
 */
class HybridPlaybackSource(
    context: Context,
    private val media: MediaSessionWatcher,
) : PlaybackSource {

    companion object {
        private const val TAG = "StaaaaandBy"
        private const val SPOTIFY_PACKAGE = "com.spotify.music"

        /**
         * MediaSession 主役のあいだの定期確認の間隔。鏡の静かな切れ(同じ曲のまま固まる、
         * 別の端末で再生が始まったのに気づかない)は手元からは見えないので、たまに
         * サーバーに聞いて確かめる。切れてから気づくまでの遅れは最大でこの長さ。
         * 45秒なら1分あたり約1.3回で、以前(約60回)の 1/45。
         */
        private const val CHECK_MS = 45_000L

        /** どちらも「何も再生していない」ときの定期確認。一晩中つけっぱなしでも呼びすぎない */
        private const val IDLE_CHECK_MS = 60_000L

        /** 外挿した位置が曲の長さをこれだけ過ぎても次の曲が来なければ API に聞く */
        private const val OVERRUN_MARGIN_MS = 3_000L

        /**
         * MediaSession が消えた/止まったあと、API に聞くまでの待ち。曲の変わり目に
         * 一瞬だけ PLAYING でなくなるのを、鏡切れと取り違えないため
         */
        private const val MEDIA_QUIET_DEBOUNCE_MS = 2_000L

        /** MediaSession 主役のあいだ、API を続けて呼ぶときの最短の間隔(ばたつき対策) */
        private const val MIN_GAP_MS = 3_000L

        /** API 主役で再生中のポーリング間隔。曲の変わり目だけはそこに合わせる */
        private const val WEB_POLL_MS = 4_000L

        /** API 主役で再生していないとき。最初は 10 秒ごと、止まったまま 3 分たったら 30 秒ごと */
        private const val WEB_PAUSED_POLL_MS = 10_000L
        private const val WEB_PAUSED_SLOW_POLL_MS = 30_000L
        private const val WEB_PAUSED_SLOW_AFTER_MS = 3 * 60_000L

        /** 再生位置がこれ以上ずれていたら食い違いとみなす(鏡はふだん 1〜2 秒遅れる) */
        private const val DRIFT_MS = 5_000L

        /** 同じ曲とみなす長さの差。MediaSession と API で数百 ms 違うことがある */
        private const val DURATION_TOLERANCE_MS = 3_000L

        /** サインインが切れているときに様子を見に行く間隔 */
        private const val AUTH_RETRY_MS = 5 * 60_000L

        /** MediaSession 主役で曲が変わってから、キューを API に聞くまでの待ち */
        private const val QUEUE_DELAY_MS = 2_000L
    }

    private enum class Lead { MEDIA, WEB }

    private val appContext = context.applicationContext
    private val web = SpotifyWebPlayer(appContext)
    private val audioManager = appContext.getSystemService(AudioManager::class.java)

    private val _nowPlaying = MutableStateFlow<NowPlaying?>(null)
    override val nowPlaying: StateFlow<NowPlaying?> = _nowPlaying

    private val _upNext = MutableStateFlow<TrackRef?>(null)
    override val upNext: StateFlow<TrackRef?> = _upNext

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private var started = false
    private var lead = Lead.MEDIA

    /** 見張りループを起こす(待ち時間を計算し直させる) */
    private val wake = Channel<Unit>(Channel.CONFLATED)

    // 出来事で頼まれた確認(定期のものとは別枠)
    private var requestedAt = 0L
    private var requestedReason: String? = null

    private var lastCheckAt = 0L
    private var nextWebPollAt = 0L
    private var webQuietSince = 0L
    private var failures = 0
    private var authBlockedUntil = 0L
    private var blockedLogged = false

    /** 終わりを過ぎた確認を済ませた MediaSession の状態(positionUpdatedAt)。同じ状態で繰り返さない */
    private var overrunCheckedFor = -1L
    private var mediaWasPlaying = false

    /** 直近1分の API 呼び出し時刻(ログ用) */
    private val recentCalls = ArrayDeque<Long>()
    private var queueJob: Job? = null

    override fun start() {
        if (started) return
        started = true
        Log.i(TAG, "hybrid: start (media session leads, Spotify Web API verifies)")
        media.start()
        scope.launch { media.nowPlaying.collect { onMediaChanged(it) } }
        scope.launch { web.nowPlaying.collect { publish() } }
        scope.launch { media.upNext.collect { publishUpNext() } }
        scope.launch { web.upNext.collect { publishUpNext() } }
        scope.launch {
            // 表示している曲が変わるたびに、次の曲(歌詞の先読み用)を用意する
            _nowPlaying.map { it?.title }.distinctUntilChanged().collect { onTrackChanged(it) }
        }
        // 開いた時点で鏡が切れていないか、まず1回だけ確かめる。定期確認はそこから数える
        lastCheckAt = SystemClock.elapsedRealtime() - MIN_GAP_MS
        request("screen opened", 0)
        scope.launch { loop() }
    }

    override fun stop() {
        Log.i(TAG, "hybrid: stop")
        scope.cancel()
        media.stop()
        web.stop()
    }

    override fun playPause() = command("playPause", { media.playPause() }, { web.playPause() })
    override fun skipToNext() = command("next", { media.skipToNext() }, { web.skipToNext() })
    override fun skipToPrevious() = command("previous", { media.skipToPrevious() }, { web.skipToPrevious() })

    private fun command(name: String, viaMedia: () -> Unit, viaWeb: suspend () -> Boolean) {
        val apiAllowed = blockedForMs(SystemClock.elapsedRealtime()) == 0L
        // MediaSession 主役でも、鏡が長く止まっていて MediaSession に play を送ると再生を
        // スマホに乗っ取りかねないとき(MediaSessionWatcher はタップを無視する)は、API に送る。
        // API なら再生していた端末に対して効く
        val viaApi = apiAllowed && (lead == Lead.WEB || !media.tapsSafe)
        Log.i(TAG, "hybrid: $name → ${if (viaApi) "API" else "media session"} (lead=$lead)")
        if (!viaApi) {
            viaMedia()
            return
        }
        scope.launch {
            if (viaWeb()) request("after $name", 250)
            else wake.trySend(Unit)   // 429 などで止められたかもしれない。ループに見直させる
        }
    }

    // ---- 見張りループ ----

    private suspend fun loop() {
        while (scope.isActive) {
            val now = SystemClock.elapsedRealtime()
            val blockedMs = blockedForMs(now)
            if (blockedMs > 0) {
                if (!blockedLogged) {
                    blockedLogged = true
                    Log.w(TAG, "hybrid: no API calls for ${blockedMs / 1000}s (rate limit or sign-in), media session only")
                }
                setLead(Lead.MEDIA, "API unavailable")
                requestedReason = null
                // 起こされても呼べないので、待ちが明けるまで寝る(長すぎる delay は刻む)
                delay(min(blockedMs, 60_000L))
                continue
            }
            if (blockedLogged) {
                blockedLogged = false
                Log.i(TAG, "hybrid: API calls allowed again")
                request("API allowed again", 0)
            }
            val (dueAt, reason) = nextCheck(now)
            if (dueAt > now) {
                withTimeoutOrNull(dueAt - now) { wake.receive() }
                continue
            }
            check(reason)
        }
    }

    private fun blockedForMs(now: Long): Long {
        if (!SpotifyAuth.isConnected(appContext)) return AUTH_RETRY_MS   // 接続を切られた。もう呼ばない
        return max(SpotifyAuth.rateLimitedForMs(appContext), authBlockedUntil - now)
    }

    /** 次に API に聞く時刻(elapsedRealtime)と、その理由 */
    private fun nextCheck(now: Long): Pair<Long, String> {
        var at = Long.MAX_VALUE
        var reason = ""
        fun consider(t: Long, r: String) {
            if (t < at) {
                at = t
                reason = r
            }
        }
        requestedReason?.let { consider(requestedAt, it) }
        if (lead == Lead.WEB) {
            consider(nextWebPollAt, "following (API leads)")
            return at to reason
        }
        val m = media.nowPlaying.value
        val idle = (m == null || !m.isPlaying) && web.nowPlaying.value?.isPlaying != true
        consider(lastCheckAt + if (idle) IDLE_CHECK_MS else CHECK_MS, if (idle) "periodic (idle)" else "periodic")
        if (m != null && m.isPlaying && m.durationMs > 0 && overrunCheckedFor != m.positionUpdatedAt) {
            val endsAt = m.positionUpdatedAt + ((m.durationMs - m.positionMs) / m.playbackSpeed).toLong()
            consider(endsAt + OVERRUN_MARGIN_MS, "past end of track, no next track from media session")
        }
        // 出来事が続いても呼びすぎない
        return max(at, lastCheckAt + MIN_GAP_MS) to reason
    }

    private fun request(reason: String, delayMs: Long) {
        requestedAt = SystemClock.elapsedRealtime() + delayMs
        requestedReason = reason
        wake.trySend(Unit)
    }

    private suspend fun check(reason: String) {
        val now = SystemClock.elapsedRealtime()
        lastCheckAt = now
        requestedReason = null
        media.nowPlaying.value?.let { m ->
            if (m.isPlaying && m.durationMs > 0 && positionAt(m, now) > m.durationMs) {
                overrunCheckedFor = m.positionUpdatedAt
            }
        }
        logCall("GET /me/player", reason)
        when (web.refresh()) {
            SpotifyWebPlayer.Result.OK -> {
                failures = 0
                evaluate()
                nextWebPollAt = SystemClock.elapsedRealtime() + webInterval()
            }
            // 再開時刻は SpotifyAuth が Prefs に保存済み。ループの頭で MediaSession に戻る
            SpotifyWebPlayer.Result.RATE_LIMITED -> Unit
            SpotifyWebPlayer.Result.AUTH -> authBlockedUntil = SystemClock.elapsedRealtime() + AUTH_RETRY_MS
            SpotifyWebPlayer.Result.FAILED -> {
                failures++
                // API 主役のときだけ急いでやり直す: 4, 8, 16, 30 秒。MediaSession 主役なら次の定期確認で
                nextWebPollAt = SystemClock.elapsedRealtime() + min(30_000L, 2_000L shl min(failures, 4))
            }
        }
    }

    /** API 主役のときの、次のポーリングまでの待ち */
    private fun webInterval(): Long {
        val now = SystemClock.elapsedRealtime()
        val w = web.nowPlaying.value
        if (w == null || !w.isPlaying) {
            if (webQuietSince == 0L) webQuietSince = now
            return if (now - webQuietSince > WEB_PAUSED_SLOW_AFTER_MS) WEB_PAUSED_SLOW_POLL_MS else WEB_PAUSED_POLL_MS
        }
        webQuietSince = 0L
        if (w.durationMs <= 0) return WEB_POLL_MS
        // 曲の終わり際はそこに合わせて聞き、次の曲へ遅れずに切り替える。
        // 終わりを過ぎたのにまだ前の曲が返ってくるあいだは1秒ごと
        val remaining = w.durationMs - positionAt(w, now)
        return when {
            remaining <= 0 -> 1_000L
            remaining < WEB_POLL_MS -> max(300L, remaining + 150)
            else -> WEB_POLL_MS
        }
    }

    // ---- 主役の判定 ----

    private fun onMediaChanged(m: NowPlaying?) {
        val playing = m?.isPlaying == true
        if (lead == Lead.WEB) {
            // 直近の API の答えと比べる(API を呼び足さない)。合っていれば鏡が戻った
            if (blockedForMs(SystemClock.elapsedRealtime()) == 0L && mismatch(m, web.nowPlaying.value) == null) {
                setLead(Lead.MEDIA, "media session agrees with API again")
            }
        } else if (mediaWasPlaying && !playing) {
            request(if (m == null) "media session gone" else "media session stopped playing", MEDIA_QUIET_DEBOUNCE_MS)
        } else if (playing && requestedReason?.startsWith("media session") == true) {
            requestedReason = null   // 曲の変わり目の一瞬だった
        }
        mediaWasPlaying = playing
        publish()
        wake.trySend(Unit)
    }

    private fun evaluate() {
        val why = mismatch(media.nowPlaying.value, web.nowPlaying.value)
        if (why != null) setLead(Lead.WEB, why) else setLead(Lead.MEDIA, "media session agrees with API")
    }

    /** MediaSession([m])と API の答え([w])が食い違っていればその理由。合っていれば null */
    private fun mismatch(m: NowPlaying?, w: NowPlaying?): String? {
        if (m != null && media.activePackage != SPOTIFY_PACKAGE) {
            // Spotify 以外の音楽アプリ。API はそれを知らないので曲は比べない。
            // そのアプリが止まっていて、Spotify がどこかで再生中のときだけ API に譲る
            return if (!m.isPlaying && w?.isPlaying == true) "other app idle, Spotify is playing elsewhere" else null
        }
        if (w == null) {
            // API: 再生中の端末なし
            if (m == null || !m.isPlaying) return null
            // スマホから実際に音が出ているなら手元を信じる(API 側の反映遅れなど)
            return if (audioManager?.isMusicActive == true) null else "media session says playing, API says nothing is playing"
        }
        if (m == null) return "no media session, API has a track"
        val sameTitle = m.title.trim().equals(w.title.trim(), ignoreCase = true)
        val sameLength = m.durationMs <= 0 || w.durationMs <= 0 ||
            abs(m.durationMs - w.durationMs) <= DURATION_TOLERANCE_MS
        if (!sameTitle || !sameLength) return "different track (media '${m.title}', API '${w.title}')"
        if (m.isPlaying != w.isPlaying) return "play state differs (media playing=${m.isPlaying}, API playing=${w.isPlaying})"
        val now = SystemClock.elapsedRealtime()
        val drift = abs(positionAt(m, now) - positionAt(w, now))
        if (drift > DRIFT_MS) return "position off by ${drift}ms"
        return null
    }

    private fun positionAt(np: NowPlaying, now: Long): Long =
        if (np.isPlaying) np.positionMs + ((now - np.positionUpdatedAt) * np.playbackSpeed).toLong() else np.positionMs

    private fun setLead(to: Lead, why: String) {
        if (lead == to) return
        Log.i(TAG, "hybrid: lead $lead → $to ($why)")
        lead = to
        if (to == Lead.WEB) {
            webQuietSince = 0L
        } else {
            // 戻った直後の確認は不要。次の定期確認までは MediaSession を信じる
            lastCheckAt = SystemClock.elapsedRealtime()
        }
        publish()
        publishUpNext()
        wake.trySend(Unit)
    }

    private fun publish() {
        _nowPlaying.value = if (lead == Lead.WEB) web.nowPlaying.value else media.nowPlaying.value
    }

    // ---- 次の曲(歌詞の先読み用) ----

    private fun publishUpNext() {
        // MediaSession のキューに次の曲があればそれ、無ければ API のキューで補う
        _upNext.value = if (lead == Lead.WEB) web.upNext.value else media.upNext.value ?: web.upNext.value
    }

    /**
     * 表示している曲が変わったら、古い「次の曲」を捨てて取り直す。
     * MediaSession 主役のときは、MediaSession のキューに次の曲が無い場合だけ API に聞く
     * (1曲につき多くて1回。Spotify 以外のアプリのときは聞かない)。
     */
    private fun onTrackChanged(title: String?) {
        queueJob?.cancel()
        web.clearUpNext()
        if (title == null) return
        queueJob = scope.launch {
            if (lead == Lead.MEDIA) {
                delay(QUEUE_DELAY_MS)
                if (lead == Lead.MEDIA && (media.upNext.value != null || media.activePackage != SPOTIFY_PACKAGE)) {
                    return@launch
                }
            }
            if (blockedForMs(SystemClock.elapsedRealtime()) > 0) return@launch
            logCall("GET /me/player/queue", "track changed, need the next track for lyrics")
            web.fetchQueue()
        }
    }

    private fun logCall(what: String, reason: String) {
        val now = SystemClock.elapsedRealtime()
        recentCalls.addLast(now)
        while (recentCalls.isNotEmpty() && now - recentCalls.first() > 60_000L) recentCalls.removeFirst()
        Log.i(TAG, "hybrid: API $what — $reason (lead=$lead, ${recentCalls.size} call(s) in the last 60s)")
    }
}
