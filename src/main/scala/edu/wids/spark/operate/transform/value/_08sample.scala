package edu.wids.spark.operate.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/10 21:30
  * 随机采样
  */
object _08sample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sample")
    val sc: SparkContext = new SparkContext(conf)
    val listRDD: RDD[Int] = sc.parallelize(1 to 10)
    //第一个参数是不放回，后面是分数界定，最后一个参数是随机数种子
    val sampleRDD: RDD[Int] = listRDD.sample(false, 0.4, 1)
    sampleRDD.collect().foreach(println)
  }

}
