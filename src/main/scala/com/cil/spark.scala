package com.cil

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/5/16.
  */
object spark {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf=new SparkConf().setAppName("pagerank").setMaster("local[2]");
    val sc=new SparkContext(conf);
    val a="file:/a/b/v/b"
    println(a.split("/").takeRight(2).head.mkString(" "))
//    val array=Array(5,8,9)
//    println(array.zipWithIndex.toBuffer)
//    val links=sc.parallelize(Array(('A',Array('D')),('B',Array('A')),('C',Array('A','B')),('D',Array('A','C'))),2)
//        .map(x=>(x._1,x._2)).cache();
//    var ranks=sc.parallelize(Array(('A',1.0),('B',1.0),('C',1.0),('D',1.0)),2);
//    println(links.join(ranks).collect().toBuffer)
//    for(i<-1 to 5){
//      val contribs=links.join(ranks,2).flatMap{
//        case(url,(links,rank))=>links.map(dest=>(dest,rank/links.size))
//      }
//      println(contribs.collect().toBuffer)
//      ranks=contribs.reduceByKey(_+_,2).mapValues(0.15+0.85*_)
//    }
//    println(ranks.collect().toBuffer)


  }
}
