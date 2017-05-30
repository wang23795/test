package com.cil

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/5/24.
  */
object window {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val ssc=new StreamingContext("local[2]","First Streaming APp",Seconds(5))

  val stream=ssc.socketTextStream("localhost",9999)

      val word=stream.map(_.split("\n"))
      val words=word.map{case a:Array[String]=>
        a.foreach(a=>print(a))}



    //val window=word.window(Seconds(10),Seconds(5))
    //window.reduceByKey((a,b)=>a+b).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
