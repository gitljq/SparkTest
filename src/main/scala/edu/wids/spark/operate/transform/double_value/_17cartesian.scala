package edu.wids.spark.operate.transform.double_value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 10:42
  * 笛卡尔积
  */
object _17cartesian {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("cartesian").setMaster("local[*]"))
    val rdd1 = sc.parallelize(1 to 3)
    val rdd2 = sc.parallelize(2 to 5)
    rdd1.cartesian(rdd2).collect().foreach(print)

  }

}
