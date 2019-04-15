package com.chaoyue.spark.project.domain

/**
  * 最近一小时窗口歌曲播放数量
  * @param dateTime_songid
  * @param play_count
  */
case class RecentlySongPlayCount(date_songid: String, play_count: Long)
