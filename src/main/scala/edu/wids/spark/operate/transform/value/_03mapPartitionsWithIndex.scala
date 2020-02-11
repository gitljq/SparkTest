package edu.wids.spark.operate.transform.value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/10 18:14
  * 关心的是分区号，
  */
object _03mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[*]").setAppName("mapPartitionsWithIndex")
      val sc = new SparkContext(conf)
//      2表示两个分区
      val listRDD = sc.makeRDD(1 to 10,2)
    val tuple = listRDD.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_, "分区号" + num))
      }
    }

    tuple.collect().foreach(println)
  }

}
