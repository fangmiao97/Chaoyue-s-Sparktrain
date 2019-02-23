package com.chaoyue.spark.project.domain

/**
  * 清洗后的日志信息
  * @param ip 日志访问ip信息
  * @param time 时间
  * @param courseId 访问的课程编号
  * @param statusCode 状态码
  * @param referer refer信息，从哪个网站引流来的
  */
case class ClickLog(ip:String, time:String, courseId:Int, statusCode:Int, referer:String)
