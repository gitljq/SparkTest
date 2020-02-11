package edu.wids.spark.operate.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 11:43
  * ：针对(K,V)类型的RDD，返回一个(K,Int)的map，表示每一个key对应的元素个数。
  */
object _40countByKey {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("countByKey").setMaster("local[*]"))
    val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    println(rdd.countByKey)
  }
}
