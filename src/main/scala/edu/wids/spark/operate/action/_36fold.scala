package edu.wids.spark.operate.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 11:39
  * 折叠操作，aggregate的简化操作，seqop和combop一样。
  */
object _36fold {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("fold").setMaster("local[*]"))
    var rdd1 = sc.makeRDD(1 to 10,2)
    println(rdd1.fold(0)(_ + _))
  }
}
