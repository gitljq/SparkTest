package edu.wids.spark.operate.transform.value

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2020/2/11 10:25
  * 管道，针对每个分区，都执行一个shell脚本，返回输出的RDD。
  * 需要在linux下操作
  * Shell脚本
    #!/bin/sh
    echo "AA"
    while read LINE; do
       echo ">>>"${LINE}
    done

  */
object _13pipe {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("pipe"))
    val listRDD = sc.parallelize(List("hi","Hello","how","are","you"),2)
    listRDD.pipe("test/pipe/pipe.sh").collect()


  }
}
