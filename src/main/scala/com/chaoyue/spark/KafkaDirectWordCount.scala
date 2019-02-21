package com.chaoyue.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder

/**
  * spark streaming整合kafka direct方式
  */
object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 2){
      System.err.println("Usage: KafkaDirectWordCount <brokers> <group> <topics> <numThreads>")
      System.exit(1)
    }

    //hadoop000:2181 test hello_ladygaga_topic 1
    val Array(brokers, topics) = args

    val sparkConf = new SparkConf()//.setAppName("KafkaDirectWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list"-> brokers)

    //TODO... 对接kafka
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc, kafkaParams, topicsSet
    )

    messages.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
