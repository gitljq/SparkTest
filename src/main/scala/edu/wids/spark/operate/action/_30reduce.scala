package edu.wids.spark.operate.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 11:28
  * 通过func函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据。
  */
object _30reduce {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("reduce").setMaster("local[*]"))
    val rdd1 = sc.makeRDD(1 to 10,2)
    println(rdd1.reduce(_ + _)) //55
    val rdd2 = sc.makeRDD(Array(("a",1),("a",3),("c",3),("d",5)))
    println(rdd2.reduce((x, y) => (x._1 + y._1, x._2 + y._2)))  //(aadc,12)
  }
}
