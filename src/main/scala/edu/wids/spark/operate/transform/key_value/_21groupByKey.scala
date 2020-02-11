package edu.wids.spark.operate.transform.key_value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 10:54
  * groupByKey也是对每个key进行操作，但只生成一个sequence。
  *
  */
object _21groupByKey {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("groupByKey"))
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))
    val group = wordPairsRDD.groupByKey()
    group.collect().foreach(print)//(one,CompactBuffer(1))(two,CompactBuffer(1, 1))(three,CompactBuffer(1, 1, 1))
    group.map(t => (t._1, t._2.sum)).collect().foreach(print)//(one,1)(two,2)(three,3)
  }

}
