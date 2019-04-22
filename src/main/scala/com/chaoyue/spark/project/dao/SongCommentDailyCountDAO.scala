package com.chaoyue.spark.project.dao

import com.chaoyue.spark.project.domain.{SongCommentDailyCount, SongLikeDailyCount}
import com.chaoyue.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object SongCommentDailyCountDAO {

  val tableName = "daily_songcomment"
  val cf = "info"
  val qualifter = "comment_count"

  /**
    * 更新每日歌曲评论数
    * @param list
    */
  def save(list: ListBuffer[SongCommentDailyCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_songid),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifter),
        ele.comment_count
      )
    }
  }

}
