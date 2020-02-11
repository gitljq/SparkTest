package edu.wids.spark.operate.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 11:44
  */
object _41foreach {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("foreach").setMaster("local[*]"))
    var rdd = sc.makeRDD(1 to 5,2)
    rdd.foreach(println(_))
  }
}
