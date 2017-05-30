package com.cil

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/5/24.
  */
object demo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf=new SparkConf()
    val sc=new SparkContext(conf.setAppName("Demo").setMaster("local[2]"))
    val a=sc.parallelize(1 to 9,3)
    println(a.fold(1)(_+_).toString)
  }
}
