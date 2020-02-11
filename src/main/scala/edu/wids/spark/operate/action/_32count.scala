package edu.wids.spark.operate.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 11:32
  * 返回RDD中元素的个数
  */
object _32count {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("count").setMaster("local[*]"))
    val rdd = sc.parallelize(1 to 10)
    println(rdd.count)
  }

}
