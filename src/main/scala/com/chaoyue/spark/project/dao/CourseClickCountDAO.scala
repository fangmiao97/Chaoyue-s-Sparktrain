package com.chaoyue.spark.project.dao

import com.chaoyue.spark.project.domain.CourseClickCount
import com.chaoyue.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 课程点击数数据访问层
  */
object CourseClickCountDAO {

  val tableName = "imooc_course_clickcount"
  val cf = "info" //columns family
  val qualifer = "click_count" //列名

  /**
    * 保存数据到HBase
    * @param list //一堆行信息
    */
  def save(list: ListBuffer[CourseClickCount]):Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for (ele <- list){//每一行的信息
      table.incrementColumnValue(Bytes.toBytes(ele.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }
  }

  /**
    * 根据rowkey查询访问量的值
    * @param day_course
    * @return
    */
  def count(day_course:String):Long = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(Bytes.toBytes(day_course))
    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)

    if (value == null){
      0l
    }else
      Bytes.toLong(value)
  }

  def main(args: Array[String]): Unit = {

    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20180906_8",8))
    list.append(CourseClickCount("20180906_4",3))
    list.append(CourseClickCount("20180906_2",2))

    save(list)

    println(count("20180906_8")+":"+count("20180906_4")+":"+count("20180906_2"))
  }
}
