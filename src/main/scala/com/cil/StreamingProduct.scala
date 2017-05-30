package com.cil

import java.io.PrintWriter
import java.net.ServerSocket

import scala.util.Random

/**
  * Created by Administrator on 2017/5/24.
  */
object StreamingProduct {

  def main(args: Array[String]): Unit = {
    val listener=new ServerSocket(9999)
    println("Listening on port:9999")
    val array=Array("cao si wa ","che si wa ","cha si wa ","she si wa ")
    val rand=new Random()
    var i=0
    while (true){
      //监听某一端口
      val socket=listener.accept()
      new Thread(){
        override def run={
          println("Got client conntected from:"+socket.getInetAddress)
          //创建输出流，向sparkstreaming那一端写数据
          val out=new PrintWriter(socket.getOutputStream(),true)
          while (true){
            Thread.sleep(1000)
            i=rand.nextInt(array.length)
              //mmp 输入端都不对，做个球 一定要是array（i）
                out.write(array(i))
                out.write("\n")
                out.flush()
            println("produce..")
            }

          socket.close()
          }

        }.start()

    }
  }
}
