package com.chaoyue.spark

import java.util.Date

import com.chaoyue.spark.project.dao.{ SongLikeDailyCountDAO}
import com.chaoyue.spark.project.domain.{SongLikeDailyCount, SongLikeLog}
import com.chaoyue.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * spark streaming对接kafka
  */
object KafkaSongLikeApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 4){
      System.err.println("Usage: KafkaStreamingApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    //hadoop000:2181 test minions_songlike 1
    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("KafkaSongLikeApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    //对接kafka
    val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
    messages.map(_._2).count().print()

    //清洗数据得到SongLikeLog
    val logs = messages.map(_._2)
    val songLikeLog = logs.map(line => {
      val infos = line.split(" ")
      val time = DateUtils.parseToMinute(infos(0)+" "+infos(1))
      val songID = infos.last.split(":")(1).dropRight(1)
      SongLikeLog(time, songID) //SongLikeLog(20190401125947,15)
    })

    //由songLikeLog统计今日歌曲收藏情况
    songLikeLog.map(x => {
      (x.time.substring(0,8) + "_" + x.songID, 1)
    }).reduceByKey(_+_).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[SongLikeDailyCount]

        partitionRecords.foreach(pair => {
          list.append(SongLikeDailyCount(pair._1, pair._2))
        })

        SongLikeDailyCountDAO.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
