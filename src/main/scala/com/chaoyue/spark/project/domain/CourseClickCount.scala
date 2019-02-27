package com.chaoyue.spark.project.domain

/**
  * 课程点击数实体类
  * @param day_course HBASE中的rowkey 20181111_1
  * @param click_count 对应这一天的访问总数
  */
case class CourseClickCount(day_course:String, click_count:Long)
