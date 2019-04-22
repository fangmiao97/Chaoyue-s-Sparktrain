package com.chaoyue.spark.project.domain

/**
  * 每日歌曲评论数实体类
  * @param day_songid
  * @param comment_count
  */
case class SongCommentDailyCount(day_songid: String, comment_count: Long)
