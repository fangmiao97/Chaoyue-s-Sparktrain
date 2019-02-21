package com.chaoyue.spark

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming整合kafka direct方式 结合flume
  */
object KafkaStreamingDirectApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      System.err.println("Usage: KafkaStreamingDirectApp <brokers> <topics>")
      System.exit(1)
    }

    //hadoop000:9092 streamingtopic
    val Array(brokers, topics) = args

    val sparkConf = new SparkConf().setAppName("KafkaStreamingDirectApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list"-> brokers)

    //TODO... 对接kafka
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc, kafkaParams, topicsSet
    )

    messages.map(_._2).count().print()

    ssc.start()
    ssc.awaitTermination()
  }
}
