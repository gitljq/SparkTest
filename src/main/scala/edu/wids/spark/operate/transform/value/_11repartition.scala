package edu.wids.spark.operate.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/10 22:08
  * 重分区
  * coalesce重新分区，可以选择是否进行shuffle过程。由参数shuffle: Boolean = false/true决定。
  * repartition实际上是调用的coalesce，默认是进行shuffle的。
  */
object _11repartition {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("repartition")
    val sc: SparkContext = new SparkContext(conf)
    val listRDD: RDD[Int] = sc.parallelize (1 to 16,4)
    println(listRDD.partitions.size)
    val repartitionRDD: RDD[Int] = listRDD.repartition(3)
    repartitionRDD.saveAsTextFile("out")
    println(repartitionRDD.partitions.size)
  }
}
