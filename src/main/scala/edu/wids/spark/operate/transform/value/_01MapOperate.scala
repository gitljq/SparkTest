package edu.wids.spark.operate.transform.value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/10 17:17
  * map是每个数据的操作,每次处理一条数据
  */
object _01MapOperate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Map")
    val sc = new SparkContext(conf)
    val listRDD = sc.makeRDD(1 to 10)
    val mapRDD = listRDD.map(_*2)
    mapRDD.collect().foreach(println)
  }

}
