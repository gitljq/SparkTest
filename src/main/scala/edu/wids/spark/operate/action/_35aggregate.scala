package edu.wids.spark.operate.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 11:37
  * aggregate函数将每个分区里面的元素通过seqOp和初始值进行聚合，
  * 然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
  * 这个函数最终返回的类型不需要和RDD中元素类型一致。
  */
object _35aggregate {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("aggregate").setMaster("local[*]"))
    var rdd1 = sc.makeRDD(1 to 10,2)
    println(rdd1.aggregate(0)(_ + _, _ + _))

  }
}
