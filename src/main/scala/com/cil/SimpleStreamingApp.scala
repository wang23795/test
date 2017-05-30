package com.cil

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/5/21.
  */
object SimpleStreamingApp {
  //从服务器套接字上读取信息，简单的输出
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val ssc=new StreamingContext("local[2]","First Streaming App",Seconds(10))
    val stream=ssc.socketTextStream("localhost",9999)
    //稍微复杂一点的流式计算
    val events=stream.map{
      record=>
        val event=record.split(",")
        (event(0),event(1),event(2))
    }
    events.foreachRDD{
      //遍历是（rdd+时间类型的）
      (rdd,time)=>
      val numPurchases=rdd.count()
        val uniqueUsers=rdd.map{case(user,_,_)=>user}.distinct().count()
        val totalRevenue=rdd.map{case(_,_,price)=>price.toDouble}.sum()
        val productsByPopularity=rdd.map{case (user,product,price)=>(product,1)}.reduceByKey(_+_).collect().sortBy(-_._2)
        val mostPupular=productsByPopularity(0)
        val formatter=new SimpleDateFormat()
        val dateStr=formatter.format(new Date(time.milliseconds))
        println(s"==Batch start time : $dateStr==")
        println("Total purchases: "+numPurchases)
        println("Unique users: "+uniqueUsers)
        println("Total revenue :"+totalRevenue)
        println("Most popular product : %s with %d purchases".format(mostPupular._1,mostPupular._2) )

    }
    //stream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
