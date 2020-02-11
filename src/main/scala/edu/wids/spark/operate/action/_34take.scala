package edu.wids.spark.operate.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 11:35
  * 返回一个由RDD的前n个元素组成的数组
  */
object _34take {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("take").setMaster("local[*]"))
    val rdd = sc.parallelize(Array(2,5,4,6,8,3))
    rdd.take(3).foreach(println)
  }

}
