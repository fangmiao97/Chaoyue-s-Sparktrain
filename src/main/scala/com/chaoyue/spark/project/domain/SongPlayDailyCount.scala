package com.chaoyue.spark.project.domain

/**
  * 每日歌曲播放数实体类
  * @param day_songid
  * @param play_count
  */
case class SongPlayDailyCount(day_songid: String, play_count: Long)
