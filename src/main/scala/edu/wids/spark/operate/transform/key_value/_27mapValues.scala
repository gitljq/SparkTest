package edu.wids.spark.operate.transform.key_value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 11:21
  * 针对于(K,V)形式的类型只对V进行操作
  */
object _27mapValues {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("mapValues"))
    val rdd3 = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))
    rdd3.mapValues(_+"|||").collect().foreach(println)


  }

}
