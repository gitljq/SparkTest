package edu.wids.spark.operate.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/10 22:08
  * 缩减分区其实就是缩减分区，没有进行shuffle。而是将相邻的分区合并了，从后往前
  */
object _10coalesce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("coalecse")
    val sc: SparkContext = new SparkContext(conf)
    val listRDD: RDD[Int] = sc.parallelize (1 to 16,4)
    println(listRDD.partitions.size)
    val coalesceRDD: RDD[Int] = listRDD.coalesce(3)
    println(coalesceRDD.partitions.size)
  }
}
