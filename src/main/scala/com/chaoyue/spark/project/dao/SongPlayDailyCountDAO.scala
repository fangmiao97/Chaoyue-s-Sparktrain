package com.chaoyue.spark.project.dao

import com.chaoyue.spark.project.domain.SongPlayDailyCount
import com.chaoyue.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object SongPlayDailyCountDAO {

  val tableName = "daily_songplay"
  val cf = "info"
  val qualifter = "play_count"

  /**
    * 更新每日歌曲播放量
    * @param list
    */
  def save(list: ListBuffer[SongPlayDailyCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_songid),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifter),
        ele.play_count
      )
    }
  }

}
