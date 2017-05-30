package com.cil

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.jblas.DoubleMatrix

/**
  * Created by Administrator on 2017/5/16.
  */
object spark02 {

  def cosSimilarity(vec1:DoubleMatrix,vec2:DoubleMatrix): Double ={
      vec1.dot(vec2)/(vec1.norm2()*vec2.norm2())
  }
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf=new SparkConf().setAppName("demo").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val originalData=sc.textFile("F:/a/ml-100k/u.data").map(_.split("\t").take(3))
    //print(originalData.first())
    val ratings=originalData.map{case Array(user,movie,rating)=>Rating(user.toInt,movie.toInt,rating.toDouble)}
    val model=ALS.train(ratings,50,10,0.01)
   // println(model.predict(789,123).toString())
    val userId=789
    val k=10
    val topK=model.recommendProducts(userId,k)
    val movies=sc.textFile("F:/a/ml-100k/u.item")
    val titles=movies.map(line=>line.split("\\|").take(2)).map(array=>(array(0).toInt,array(1))).collectAsMap()
    //println(topK.mkString("\n"))
    //println(ratings.keyBy(_.user).lookup(789).sortBy(-_.rating).take(10).map(rating=>(titles(rating.product),rating.rating)).foreach(println))
    val itemid=567
    val itemFactor=model.productFeatures.lookup(itemid).head
    val itemVector=new DoubleMatrix(itemFactor)
    val sims=model.productFeatures.map{case(id,factor)=>
    val factorVector=new DoubleMatrix(factor)
      val sim=cosSimilarity(factorVector,itemVector)
      (id,sim)
    }
    val sortedSims=sims.top(10)(Ordering.by[(Int,Double),Double]{case (id,similarity)=>similarity})
    //println(sortedSims.take(10).mkString("\n"))
    //println(titles(itemid))
    val sortedSims2=sims.top(k+1)(Ordering.by[(Int,Double),Double]{case(id,similarity)=>similarity})
    println(sortedSims2.slice(1,11).map{case(id,sim)=>(titles(id),sims)}.mkString("\n"))
  }

}
