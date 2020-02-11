package edu.wids.spark.operate.transform.double_value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 10:32
  * 并集123455678910
  */
object _14union {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("union").setMaster("local[*]"))
    val rdd1 = sc.parallelize(1 to 5)
    val rdd2 = sc.parallelize(5 to 10)
    rdd1.union(rdd2).collect().foreach(print)
  }

}
