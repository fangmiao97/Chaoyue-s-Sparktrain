package com.chaoyue.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * updateStateByKey
  * 统计到目前为止累计出现的单词的个数(需要保持住以前的状态)
  */

object StatefulWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 如果使用了stateful的算子，必须设置checkpoint
    // 在生产环境中，建议大家把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("localhost", 6789)
    val result = lines.flatMap(_.split(" ")).map((_, 1))
    val state = result.updateStateByKey[Int](updateFunction _)
    state.print()

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 把当前的数据去更新已有的或者是老的数据
    * @param currentValues
    * @param preValues
    * @return
    */
  def updateFunction(currentValues: Seq[Int], preValues:Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current+pre)
  }
}


