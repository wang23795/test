package com.cil

import java.io.PrintWriter
import java.net.ServerSocket

import scala.util.Random

/**
  * Created by Administrator on 2017/5/21.
  */
object spark06 {
  def main(args: Array[String]): Unit = {
    //随机生成用户，产品，价格
    val random=new Random();
    val MaxEvents=6
    val nameResource=this.getClass.getResourceAsStream("/names.csv")
    val names=scala.io.Source.fromInputStream(nameResource).getLines().toList.head.split(",").toSeq
    val products=Seq("iPhone Cover"->9.99,"Headphones"->5.49,"Samsung Galaxy Cover"->8.95,"iPad Cover"->7.49)
    def generateProductEvents(n:Int)={
      (1 to n).map{i=>
        val (product,price)=products(random.nextInt(products.size))
        val user=random.shuffle(names).head
        (user,product,price)
      }
    }
    //生成网络生成器,监听9999端口，如果有发送来的消息后会随机成成用户，产品，价格对
    val listener=new ServerSocket(9999)
    println("Listening on port :9999")
    while(true){
      val socket=listener.accept()
      new Thread(){
        override def run()={
          println("Got client connected from :"+socket.getInetAddress)
          val out=new PrintWriter(socket.getOutputStream,true)
          while(true){
            Thread.sleep(1000)
            val num=random.nextInt(MaxEvents)
            val productEvents=generateProductEvents(num)
            productEvents.foreach{
              event=>
                out.write(event.productIterator.mkString(","))
                out.write("\n")
            }
            out.flush()
            println(s"Create $num events...")
          }
          socket.close()
        }

      }.start()
    }
  }
}
