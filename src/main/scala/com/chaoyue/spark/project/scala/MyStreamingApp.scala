package com.chaoyue.spark.project.scala

import com.chaoyue.spark.project.domain.ClickLog
import com.chaoyue.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 全链路打通
  */
object MyStreamingApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 4){
      System.err.println("Usage: MyStreamingApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("MyStreamingApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    //TODO...
    val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

//    messages.map(_._2).count().print()

    //测试二数据清洗
    val logs = messages.map(_._2)
    val cleanData = logs.map(line => {
      val infos = line.split("\t")//把每行日志根据\t分隔符分割
      val url = infos(2).split(" ")(1)
      // /class/128.html
      var courseID = 0

      if (url.startsWith("/class")){//class开头的把课程编号拿出来
        val courseIdHTML = url.split("/")(2)
        courseID = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseID, infos(3).toInt, infos(4))
    }).filter( clicklog => clicklog.courseId != 0)

    cleanData.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
