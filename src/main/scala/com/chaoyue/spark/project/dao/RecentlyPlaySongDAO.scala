package com.chaoyue.spark.project.dao

import com.chaoyue.spark.project.domain.RecentlySongPlayCount
import com.chaoyue.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object RecentlyPlaySongDAO {

  val tablename = "recently_play_song_hour"
  val cf = "info"
  val qualifter = "play_count"

  def save(list: ListBuffer[RecentlySongPlayCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tablename)

    for (ele <- list) {

      val row = new Put(Bytes.toBytes(ele.date_songid))
      row.add(Bytes.toBytes(cf), Bytes.toBytes(qualifter), Bytes.toBytes(ele.play_count))

      table.put(row)
    }

    table.close()
  }
}
