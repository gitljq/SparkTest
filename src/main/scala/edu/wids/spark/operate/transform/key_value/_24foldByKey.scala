package edu.wids.spark.operate.transform.key_value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 11:12
  * aggregateByKey的简化操作
  */
object _24foldByKey {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("foldByKey"))
    val rdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    val agg = rdd.foldByKey(0)(_+_)
    agg.collect().foreach(println)
  }

}
