package com.chaoyue.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * updateStateByKey
  * 统计到目前为止累计出现的单词的个数(需要保持住以前的状态)
  */

object ForeachRDDApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 6789)
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    //TODO... 将结果写到MySQL中
    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          val sql = "update INTO wordCount(word, word_Count) VALUES ('" + record._1 +"'," + record._2 +")"
          connection.createStatement().execute(sql)
        })
        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * MySql连接
    * @return
    */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://101.132.45.245： 3306/MessageWikiPro","root","cyf970117")
  }
}


