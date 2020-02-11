package edu.wids.spark.operate.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 11:31
  * 在驱动程序中，以数组的形式返回数据集的所有元素。
  */
object _31collect {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("collect").setMaster("local[*]"))
    val rdd = sc.parallelize(1 to 10)
    println(rdd.collect)
  }

}
