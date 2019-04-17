package com.chaoyue.spark.project.dao

import java.util.Date

import com.chaoyue.spark.project.dao.CourseClickCountDAO.{count, save}
import com.chaoyue.spark.project.domain.{ClickCountTrend, CourseClickCount}
import com.chaoyue.spark.project.utils.HBaseUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 访问数据每隔10分钟记录趋势表访问层
  */
object ClickCountTrendDAO {

  val tablename = "clickcount_trend"
  val cf = "info"
  val qualifer = "click_count"

  def save(list: ListBuffer[ClickCountTrend]): Unit = {
    val table = HBaseUtils.getInstance().getTable(tablename)

    for(ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_time_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }
  }

  def main(args: Array[String]): Unit = {

    val list = new ListBuffer[ClickCountTrend]
    list.append(ClickCountTrend("20180906_8",8))
    list.append(ClickCountTrend("20180906_4",3))
    list.append(ClickCountTrend("20180906_2",2))

    save(list)

    println(count("20180906_8")+":"+count("20180906_4")+":"+count("20180906_2"))
  }
}
