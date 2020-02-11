package edu.wids.spark.operate.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/10 20:57
  *
  * 将每个分区形成一个数组，形成新的RDD类型RDD[Array[T]]
  * 创建一个4个分区的RDD，并且把每个分区的数据放到一个数组
  */
object _05glom {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("glom").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val listRDD: RDD[Int] = sc.parallelize(1 to 19,4)
//    将一个分区的数据放到一个数组中
    val glomRDD: RDD[Array[Int]] = listRDD.glom()
    glomRDD.collect().foreach(array=>{
      print(array.mkString(","))
      println()
    }
    )


  }

}
