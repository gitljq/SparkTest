package edu.wids.spark.operate.transform.double_value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 10:45
  * 拉链
  * 分区数应该保持一致，两个rdd数据元素数量一致
  */
object _19zip {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("zip"))
    val rdd1 = sc.parallelize(Array(1,2,3),3)
    val rdd2 = sc.parallelize(Array("a","b","c"),4)
    rdd1.zip(rdd2).collect.foreach(println)
  }
}
