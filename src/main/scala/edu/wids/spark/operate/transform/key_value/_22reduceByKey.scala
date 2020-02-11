package edu.wids.spark.operate.transform.key_value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 11:00
  * 在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，将相同key的值聚合到一起，
  * reduce任务的个数可以通过第二个可选的参数来设置。
  */
object _22reduceByKey {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("reduceByKey"))

    val rdd = sc.parallelize(List(("female",1),("male",5),("female",5),("male",2)))
    val reduce = rdd.reduceByKey((x,y) => x+y)
    reduce.collect().foreach(println)
  }
}
