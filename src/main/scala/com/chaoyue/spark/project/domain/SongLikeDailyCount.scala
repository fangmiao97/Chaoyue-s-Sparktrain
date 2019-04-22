package com.chaoyue.spark.project.domain

/**
  * 每日歌曲收藏数实体类
  * @param day_songid
  * @param like_count
  */
case class SongLikeDailyCount(day_songid: String, like_count: Long)
