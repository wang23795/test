package com.cil

//import breeze.linalg.DenseVector
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
import breeze.linalg.DenseVector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/5/21.
  */
//object SimpleStreamingModel {
//  def main(args: Array[String]): Unit = {
//    val ssc=new StreamingContext("local[2]","First Streaming App",Seconds(10))
//    val stream=ssc.socketTextStream("localhost",9999)
//
//    val numFeatures=100
//    val zeroVector=DenseVector.zeros[Double](numFeatures)
//    val model=new StreamingLinearRegressionWithSGD().
//      setInitialWeights(Vectors.dense(zeroVector.data)).
//      setNumIterations(1).
//      setStepSize(0.01)
//    val labeledStream=stream.map{event=>
//      val split=event.split("\t")
//      val y=split(0).toDouble
//      val features=split(1).split(",").map(_.toDouble)
//      LabeledPoint(label = y,features=Vectors.dense(features))
//    }
//    model.trainOn(labeledStream)
//    model.predictOn(labeledStream)
//  }
//
//}
object SimpleStreamingModel {

  def main(args: Array[String]) {
    //设定日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val ssc = new StreamingContext("local[2]", "First Streaming App", Seconds(10))
    val stream = ssc.socketTextStream("localhost", 9999)
    //定义特征维数
    val NumFeatures = 100
    //将初始权重设置为0,0,0,0....
    val zeroVector = DenseVector.zeros[Double](NumFeatures)
    //初始化权重向量，迭代次数，步长
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.dense(zeroVector.data))
      .setNumIterations(1)
      .setStepSize(0.01)

    // create a stream of labeled points
    //从服务器端读取数据构造成label的形式，（y,x）LabeledPoint(y,x)
    val labeledStream = stream.map { event =>
      val split = event.split("\t")
      val y = split(0).toDouble
      val features = split(1).split(",").map(_.toDouble)
      LabeledPoint(label = y, features = Vectors.dense(features))
    }
    //构造predictx预测的形式，一定要改成spark Vectors的形式，这个地方费了点劲
    val predictx = stream.map { event =>
      val split = event.split("\t")
      val features = split(1).split(",").map(_.toDouble)
      Vectors.dense(features)
    }

    // train and test model on the stream, and print predictions for illustrative purposes
    //训练
    model.trainOn(labeledStream)
    //预测输出
    model.predictOn(predictx).print()

    ssc.start()
    ssc.awaitTermination()

  }
}