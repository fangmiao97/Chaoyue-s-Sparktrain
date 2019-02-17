package com.chaoyue.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用spark streaming处理文件系统（HDFS/local）的数据
  */
object FileWordCount {

  def main(args: Array[String]): Unit = {

    //这里可以写local 因为是文件系统，已经持久化过了，不需要额外的线程
    val sparkConf = new SparkConf().setMaster("local").setAppName("FileWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.textFileStream("file:///Users/chaoyue/data/sparktest/")

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
