package edu.wids.spark.operate.transform.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/10 21:47
  * 去重，数据重组，数据分区打乱，shuffle
  */
object _09distinct {
    def main(args: Array[String]): Unit = {
      val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("distinct")
      val sc: SparkContext = new SparkContext(conf)
      val listRDD: RDD[Int] = sc.parallelize (List(1,1,1,2,5,2,2,7))
//      val distinctRDD: RDD[Int] = listRDD.distinct()
//      参数指定为2的时候是去重后数据减少后分到2个分区。
      val distinctRDD: RDD[Int] = listRDD.distinct(2)
      distinctRDD.collect().foreach(println)
    }

}
