package com.chaoyue.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 黑名单过滤
  */
object TransformAPP {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))//连个参数sparkConf和batch interval

    //构建黑名单,一般讲黑名单放在数据库中，之后转成RDD
    val blacks = List("zs", "ls")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x => (x, true))

    val lines = ssc.socketTextStream("localhost", 6789)
    val clicklogs = lines.map(x => (x.split(",")(1), x)).transform(rdd =>{
      rdd.leftOuterJoin(blacksRDD)
        .filter(x => x._2._2.getOrElse(false) != true)//把join之后黑名单不等于true的拿出来
        .map(x => x._2._1)//把日志拿出来
    })

    clicklogs.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
