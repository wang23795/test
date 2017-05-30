package com.cil

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/5/21.
  */

//有状态的流式计算

object StreamingStateApp {
  //这个地方的自定义函数将每一批次的相同key对应的值放到Seq中进项处理，此处是（string，double）=》(product,price)对，还有一个以前的
  //值存放在currentTotal中，这个是在自己函数中自定义类型的，最后返回some类型
  def updateState(prices:Seq[(String,Double)],currentTotal:Option[(Int,Double)])={
    val currentRevenue=prices.map(_._2).sum
    val currentNumberPurchases=prices.size
    val state=currentTotal.getOrElse((0,0.0))
    Some((currentNumberPurchases+state._1,currentRevenue+state._2))
  }
  //自定义updateStae函数，进行批次的累计操作
  def main(args: Array[String]): Unit = {
    import org.apache.spark.streaming.StreamingContext._
    val ssc=new StreamingContext("local[2]","UpdateState",Seconds(10))
    ssc.checkpoint("F:/b")
    val stream=ssc.socketTextStream("localhost",9999)
      //由于event(2)后面忘记了。todouble（）导致一直报错，细心得
    val events=stream.map{
      record=>
        val event=record.split(",")
        (event(0),event(1),event(2).toDouble)
    }
    val users=events.map{
      case(user,product,price)=>(user,(product,price))
    }
    //updateStateByKey根据键值对像是来进行更新，因此上一步必须是kv对
    val revenuePerUser=users.updateStateByKey(updateState)
    revenuePerUser.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
