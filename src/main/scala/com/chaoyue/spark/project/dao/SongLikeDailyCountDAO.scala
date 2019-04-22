package com.chaoyue.spark.project.dao

import com.chaoyue.spark.project.domain.{SongLikeDailyCount}
import com.chaoyue.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object SongLikeDailyCountDAO {

  val tableName = "daily_songlike"
  val cf = "info"
  val qualifter = "like_count"

  /**
    * 更新每日歌曲收藏量
    * @param list
    */
  def save(list: ListBuffer[SongLikeDailyCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_songid),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifter),
        ele.like_count
      )
    }
  }

}
