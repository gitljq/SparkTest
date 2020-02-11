package edu.wids.spark.operate.transform.value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/10 17:24
  * 对每个分区进行操作，分区数据处理完后，分区的数据才会释放，可能会导致OOM，性能比map好
  */
object _02MapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions")
    val sc = new SparkContext(conf)
    val listRDD = sc.makeRDD(1 to 10)
//    参数是一个迭代的对象，就是把每个分区的数据拿来做mapPartitions
//    mapPartitions效率优于map算子，减少了发送到执行器的交互次数
//    可能会内存溢出
    val mapPartitionsRDD = listRDD.mapPartitions(date=>{
      date.map(date=>date*2)
    })
    mapPartitionsRDD.collect().foreach(println)
  }
}
