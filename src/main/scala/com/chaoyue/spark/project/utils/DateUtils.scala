package com.chaoyue.spark.project.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期工具类
  */
object DateUtils {

  val OLD_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  val TARGET_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time: String) = {
    OLD_FORMAT.parse(time).getTime
  }

  def parseToMinute(time: String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    println(parseToMinute("2019-04-01 11:52:04,488"))
  }
}
