package edu.wids.spark.operate.transform.key_value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 11:23
  * 在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD
  */
object _28join {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("join"))
    val rdd = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val rdd1 = sc.parallelize(Array((1,4),(2,5),(3,6)))
    rdd.join(rdd1).collect().foreach(println)
  }
}
