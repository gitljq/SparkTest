package edu.wids.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/8 21:46
  */
object _01WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.textFile("test/wordcount")
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map((_,1))
    val rdd4 = rdd3.reduceByKey(_+_)
    rdd4.foreach(print)
  }
}
