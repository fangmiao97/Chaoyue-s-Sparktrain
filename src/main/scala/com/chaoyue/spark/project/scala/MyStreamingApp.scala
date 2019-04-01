package com.chaoyue.spark.project.scala

import com.chaoyue.spark.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.chaoyue.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.chaoyue.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

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

    val sparkConf = new SparkConf().setAppName("MyStreamingApp").setMaster("local[5]")
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

    //cleanData.print()//ClickLog(87.30.187.55,20190219090601,145,404,-)

    //三、统计到今天到现在为止的课程访问量
    cleanData.map(x => {
      (x.time.substring(0,8)+"_"+x.courseId, 1)
    }).reduceByKey(_+_).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })

        CourseClickCountDAO.save(list)
      })
    })

    //四、统计从搜索引擎过来的从今天开始到现在的课程的访问量
    //x: ClickLog(72.124.63.29,20190219090601,112,404,http://cn.bing.com/search?q=Storm实战)
    cleanData.map(x => {
      val referer = x.referer.replaceAll("//", "/")
      val splits = referer.split("/")
      var host = "" //有的clicklog是没有referer的，就是 -
      if(splits.length>2){
        host = splits(1)
      }

      (host, x.courseId, x.time)
    }).filter(_._1 != "").map(x => {
      (x._3.substring(0,8)+"_"+x._1+"_"+x._2, 1)
    }).reduceByKey(_+_).foreachRDD( rdd => {
      rdd.foreachPartition(partitonRecords => {
        val list = new ListBuffer[CourseSearchClickCount]

        partitonRecords.foreach(pair => {
          list.append(CourseSearchClickCount(pair._1, pair._2))
        })
        CourseSearchClickCountDAO.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
