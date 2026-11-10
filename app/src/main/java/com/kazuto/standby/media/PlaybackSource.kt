package com.kazuto.standby.media

import kotlinx.coroutines.flow.StateFlow

/**
 * スタンバイ画面が表示・操作する「いま再生中のもの」の供給元。
 *  - [MediaSessionWatcher]: 端末上の音楽アプリの MediaSession(他端末の鏡を含む)
 *  - [com.kazuto.standby.spotify.HybridPlaybackSource]: Spotify に接続済みのとき。MediaSession を
 *    主役にし、Spotify Web API でたまに答え合わせをして、食い違ったら Web API に切り替える
 */
/** 次に再生される曲の手がかり。歌詞の先読みに使う。durationMs は不明なら 0 */
data class TrackRef(val title: String, val artist: String, val album: String?, val durationMs: Long)

interface PlaybackSource {
    val nowPlaying: StateFlow<NowPlaying?>
    /** 次に再生される曲。分からなければ null */
    val upNext: StateFlow<TrackRef?>
    fun start()
    fun stop()
    fun playPause()
    fun skipToNext()
    fun skipToPrevious()
}
