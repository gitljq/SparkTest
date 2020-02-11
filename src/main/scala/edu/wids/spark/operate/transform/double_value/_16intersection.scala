package edu.wids.spark.operate.transform.double_value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 10:38
  * 交集
  * 567
  */
object _16intersection {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("intersection").setMaster("local[*]"))
    val rdd1 = sc.parallelize(1 to 7)
    val rdd2 = sc.parallelize(5 to 10)
    val rdd3 = rdd1.intersection(rdd2)
    rdd3.collect().foreach(print)
  }
}
