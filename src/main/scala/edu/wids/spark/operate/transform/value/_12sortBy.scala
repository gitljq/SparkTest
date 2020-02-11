package edu.wids.spark.operate.transform.value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 10:17
  * 排序
  */
object _12sortBy {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("sortBy"))
    val listRDD = sc.parallelize(List(2,1,3,4))
//    按照自身大小排序
    listRDD.sortBy(x=>x).collect().foreach(print)
    println()
//    按照与3余数的大小排序
    listRDD.sortBy(x => x%3).collect().foreach(print)


  }

}
