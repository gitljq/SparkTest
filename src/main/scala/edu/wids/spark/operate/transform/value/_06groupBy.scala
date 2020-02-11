package edu.wids.spark.operate.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/10 21:13
  * 按照传入函数的返回值分组,将相同的返回值放到同一组
  *
  */
object _06groupBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("groupBy").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val listRDD: RDD[Int] = sc.parallelize(1 to 10)
//    返回值形成了对偶元组，key表示分组key，即数组执行函数的返回值，v为分组的元素集合
    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(_%3)
    groupByRDD.collect().foreach(println)

//    返回值如下
//    (0,CompactBuffer(3, 6, 9))
//    (1,CompactBuffer(1, 4, 7, 10))
//    (2,CompactBuffer(2, 5, 8))
  }

}
