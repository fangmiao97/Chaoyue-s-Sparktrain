package com.chaoyue.spark

import java.util.Date

import com.chaoyue.spark.project.dao.{RecentlyPlaySongDAO, SongPlayDailyCountDAO}
import com.chaoyue.spark.project.domain.{RecentlySongPlayCount, SongPlayDailyCount, SongPlayLog}
import com.chaoyue.spark.project.utils.DateUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * spark streaming对接kafka
  */
object KafkaStreamingApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 4){
      System.err.println("Usage: KafkaStreamingApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    //hadoop000:2181 test minions_songplay 1
    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("KafkaStreamingApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    //对接kafka
    val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
    messages.map(_._2).count().print()

    //清洗数据得到SongLog
    val logs = messages.map(_._2)
    val songPlayLog = logs.map(line => {
      val infos = line.split(" ")
      val time = DateUtils.parseToMinute(infos(0)+" "+infos(1))
      val songID = infos.last.split(":")(1).dropRight(1)
      SongPlayLog(time, songID) //SongPlayLog(20190401125947,15)
    })

    //由SongPlayLog统计今日歌曲播放情况
    songPlayLog.map(x => {
      (x.time.substring(0,8) + "_" + x.songID, 1)
    }).reduceByKey(_+_).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[SongPlayDailyCount]

        partitionRecords.foreach(pair => {
          list.append(SongPlayDailyCount(pair._1, pair._2))
        })

        SongPlayDailyCountDAO.save(list)
      })
    })

    //统计最近一个小时歌曲播放情况，每十分钟更新一次，数据库中只保留最近一个小时的数据
    songPlayLog.map(x => {
      (x.time.substring(0,8) + "_" + x.songID, 1)
    }).reduceByKeyAndWindow((x: Int, y: Int) => x + y,
      Seconds(3600), Seconds(600)).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[RecentlySongPlayCount]

        partitionRecords.foreach(pair => {
          list.append(RecentlySongPlayCount(pair._1,
            pair._2))
        })
        RecentlyPlaySongDAO.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
