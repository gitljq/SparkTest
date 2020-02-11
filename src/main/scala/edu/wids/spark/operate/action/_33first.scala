package edu.wids.spark.operate.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 11:33
  * 返回RDD中的第一个元素
  */
object _33first {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("first").setMaster("local[*]"))
    val rdd = sc.parallelize(1 to 10)
    println(rdd.first)
  }
}
