package com.cil

import breeze.linalg.DenseVector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/5/22.
  */
object MonitoringStreamingModel {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val ssc=new StreamingContext("local[2]","First Streaming APp",Seconds(10))
    val stream=ssc.socketTextStream("localhost",9999)

    val numFeatures=100
    val zeroVector=DenseVector.zeros[Double](numFeatures)
    val model1=new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(zeroVector.data))
      .setNumIterations(1)
      .setStepSize(0.01)
    val model2=new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(zeroVector.data))
      .setNumIterations(1)
      .setStepSize(1.0)
    val labeledStream=stream.map{
      event=>
        val split=event.split("\t")
        val y=split(0).toDouble
        val features=split(1).split(",").map(_.toDouble)
        LabeledPoint(label = y,features=Vectors.dense(features))
    }
    model1.trainOn(labeledStream)
    model2.trainOn(labeledStream)

    val predsAndTrue=labeledStream.transform{
      rdd=>
        val latest1=model1.latestModel()
        val latest2=model2.latestModel()
        rdd.map{
          point=>
            val pred1=latest1.predict(point.features)
            val pred2=latest2.predict(point.features)
            (pred1-point.label,pred2-point.label)
        }
    }
    //计算每个模型的误差跟均方误差，一个模型步长合适，迭代多次后误差会变小，另一个由于步长不合适，误差不怎么变
    predsAndTrue.foreachRDD{
      (rdd,time)=>
        val mse1=rdd.map{case(err1,err2)=>err1*err1}.mean()
        val rmse1=math.sqrt(mse1)
        val mse2=rdd.map{case(err1,err2)=>err2*err2}.mean()
        val rmse2=math.sqrt(mse2)
        println(
          s"""
            |--------------------------------------------------------
            |Time:$time
            |--------------------------------------------------------
            """.stripMargin
        )
        println(s"MSE current batch:Model1 :$mse1;Model2 :$mse2")
        println(s"RMSE current batch:Model1 :$rmse1;Model2 :$rmse2")
        println("...\n")
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
