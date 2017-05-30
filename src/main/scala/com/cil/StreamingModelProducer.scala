package com.cil

import java.io.PrintWriter
import java.net.ServerSocket

import breeze.linalg.DenseVector

import scala.util.Random

/**
  * Created by Administrator on 2017/5/21.
  */
object StreamingModelProducer {
  def main(args: Array[String]): Unit = {

    val MaxEvent=100
    val NumFeatures=100
    val random=new Random()
    //生成服从正态分布的稠密向量的函数,就是生成向量里面每一个数字（n）个
    def generateRandomArray(n:Int)=Array.tabulate(n)(_=>random.nextGaussian())
    //生成一个确定的随机模型权重向量(w跟b)w为n维，b只有一个
    val w=new DenseVector(generateRandomArray(NumFeatures))
    val intercept=random.nextGaussian()*10
    //随机生成一些NumFeatures维的点，根据y=x*w+b算出他们的y，返回结果
    //此处生成了n个数据点，每个数据点都构造成向量，根据x算出y然后返回结果
    def generateNoisyData(n:Int)={
      (1 to n).map{
        i=>
          val x=new DenseVector(generateRandomArray(NumFeatures))
          val y:Double=w.dot(x)
          val noisy=y+intercept
          (noisy,x)
      }
    }
    //套接字程序，服务器端
    val listener=new ServerSocket(9999)
    println("Listening on port:9999")
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
            val num=random.nextInt(MaxEvent)
            //一次生成num个数据点发送过去
            val data=generateNoisyData(num)
            //对发送的数据进行遍历做一些处理
            //向那边发送的数据格式为 y x的形式
            data.foreach{
              case(y,x)=>
                val xStr=x.data.mkString(",")
                val eventStr=s"$y\t$xStr"
               //向客户端写模拟的数据（out）
                out.write(eventStr)
                out.write("\n")
            }
            out.flush()
            println(s"Created $num event...")
          }
          socket.close()
        }
      }.start()
    }
  }
}
