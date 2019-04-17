package com.chaoyue.spark.project.domain

/**
  * 每十分钟各类目访问量的统计 实体类
  * @param day_time_course rowkey
  * @param click_count
  */
case class ClickCountTrend(day_time_course: String, click_count: Long)
