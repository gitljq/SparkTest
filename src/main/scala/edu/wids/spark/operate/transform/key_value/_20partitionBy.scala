package edu.wids.spark.operate.transform.key_value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 10:49
  * 对pairRDD进行分区操作，如果原有的partionRDD和现有的partionRDD是一致的话就不进行分区，
  * 否则会生成ShuffleRDD，即会产生shuffle过程。
  */
object _20partitionBy {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("partitionBy"))
    val rdd = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)
    println(rdd.partitions.size)
    var rdd2 = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))
    println(rdd2.partitions.size)
  }

}
