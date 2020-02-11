package edu.wids.spark.operate.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/10 21:21
  *返回传入函数返回true的数据
  */
object _07filter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("filter").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val listRDD: RDD[Int] = sc.parallelize(1 to 10)
//    和scala的filter一样,取%2的数
    val filterRDD: RDD[Int] = listRDD.filter(x=>x%2==0)
    filterRDD.collect().foreach(println)
  }

}
