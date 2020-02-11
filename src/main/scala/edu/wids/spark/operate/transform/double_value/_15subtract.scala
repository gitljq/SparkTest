package edu.wids.spark.operate.transform.double_value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 10:35
  * 计算差的一种函数，去除两个RDD中相同的元素，不同的RDD将保留下来,调用者减去两RDD交集
  * 678
  */
object _15subtract {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("subtract").setMaster("local[*]"))
    val rdd = sc.parallelize(3 to 8)
    val rdd1 = sc.parallelize(1 to 5)
    rdd.subtract(rdd1).collect().foreach(print)
  }
}
