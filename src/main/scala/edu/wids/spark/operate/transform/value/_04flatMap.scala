package edu.wids.spark.operate.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/10 18:14
  * 每个输入映射成0或多个输出
  */
object _04flatMap {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[*]").setAppName("flatMap")
      val sc = new SparkContext(conf)
      val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1,2),List(3,4)))
      val flatMapRDD = listRDD.flatMap(data=>data)
      flatMapRDD.collect().foreach(print)
  }

}
