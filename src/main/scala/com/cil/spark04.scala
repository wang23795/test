package com.cil

import breeze.linalg.DenseVector
import breeze.numerics.pow
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/5/19.
  */
object spark04 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf=new SparkConf().setAppName("k-mean").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val movies=sc.textFile("F:/a/ml-100k/u.item")
    val genres=sc.textFile("F:/a/ml-100k/u.genre")
    val genreMap=genres.filter(!_.isEmpty).map(line=>line.split("\\|")).map(array=>(array(1),array(0))).collectAsMap()
    //println(genreMap.toString())
    //弄成（序号，（电影，类型1，类型2....的格式））
    val titlesAndGenres=movies.map(_.split("\\|")).map{
      array=>
        val genres=array.toSeq.slice(5,array.size)
        val genresAssigned=genres.zipWithIndex.filter{
          case(g,idx)=>
            g=="1"
        }.map{
          case(g,idx)=>genreMap(idx.toString)
        }
        (array(0).toInt,(array(1),genresAssigned))
    }
    //println(titlesAndGenres.first().toString())
    val rawData=sc.textFile("F:/a/ml-100k/u.data")
    val rawRatings=rawData.map(_.split("\t").take(3))
    val ratings=rawRatings.map{case Array(user,movie,rating)=>Rating(user.toInt,movie.toInt,rating.toDouble)}
    ratings.cache()

    //50个隐变量，10次迭代，0.1的正则化系数,将这些隐含变量抽取出来做为特征进行聚类
    val alsModel=ALS.train(ratings,50,10,0.1)
    val movieFactors=alsModel.productFeatures.map{case (id,factor)=> (id,Vectors.dense(factor))}
    val movieVectors=movieFactors.map(_._2)
    val userFactors=alsModel.userFeatures.map{case (id,factor)=>(id,Vectors.dense(factor))}
    val userVectors=userFactors.map(_._2)
   //看看这些隐含变量的均值方差啥的
    val movieMatrix=new RowMatrix(movieVectors)
    val movieMatrixSummary=movieMatrix.computeColumnSummaryStatistics()
    val userMatrix=new RowMatrix(userVectors)
    val userMatrixSummary=userMatrix.computeColumnSummaryStatistics()
//    println("movie factors mean"+movieMatrixSummary.mean)
//    println("movie factors variance"+movieMatrixSummary.variance)
//    println("user factors mean"+userMatrixSummary.mean)
//    println("user factors variance"+userMatrixSummary.variance)
    //仅限训练，k=5,迭代次数10，训练次数3
    val numClusters=5
    val numIterations=10
    val numRuns=3
    val movieClusterModel=KMeans.train(movieVectors,numClusters,numIterations,numRuns)
    val userClusterModel=KMeans.train(userVectors,numClusters,numIterations,numRuns)
    //进行预测,取一条数据;预测多个
//    val movie1=movieVectors.first()
//    val movie1Clusger=movieClusterModel.predict(movie1)
//    println(movie1Clusger)
//    val predictions=movieClusterModel.predict(movieVectors)
//    println(predictions.take(10).mkString(","))
//    //进行一些评估,取出每个聚类中心前20的数据（将电影基本信息与电影聚类信息进行join,看看效果）
//    def computeDistance(v1:DenseVector[Double],v2:DenseVector[Double])=pow(v1-v2,2).sum
//    val titlesWithFactors=titlesAndGenres.join(movieFactors)
//    val moviesAssigned=titlesWithFactors.map{
//      case(id,((title,genres),vector))=>
//        val pred=movieClusterModel.predict(vector)
//        val clusterCentre=movieClusterModel.clusterCenters(pred)
//        val dist=computeDistance(DenseVector(clusterCentre.toArray),DenseVector(vector.toArray))
//        (id,title,genres.mkString(" "),pred,dist)
//
//    }
//    val clusterAssignments=moviesAssigned.groupBy{case(id,title,genres,cluster,dist)=>cluster}.collectAsMap()
//    for((k,v)<-clusterAssignments.toSeq.sortBy(_._1)){
//      println(s"Cluster $k")
//      val m=v.toSeq.sortBy(_._5)
//      println(m.take(20).map{
//        case (_,title,genres,_,d)=>
//          (title,genres,d)
//      }.mkString("\n"))
//      println("==========\n")
//    }
    //kmean的一些性能指标wcss
    val movieCost=movieClusterModel.computeCost(movieVectors)
    val userCost=userClusterModel.computeCost(userVectors)
    println("WCSS for movie: "+movieCost)
    println("WCSS for users: "+userCost)
//通过交叉验证选择K(电影的)
    val trainTestSplitMovies=movieVectors.randomSplit(Array(0.6,0.4),123)
    val trainMovies=trainTestSplitMovies(0)
    val testMovies=trainTestSplitMovies(1)
    val costsMovies=Seq(2,3,4,5,10,20).map{
      k=>(k,KMeans.train(trainMovies,numIterations,k,numRuns).computeCost(testMovies))
    }
    println("Movie clustering cross-validation:")
    costsMovies.foreach{
      case (k,cost)=>println(f"WCSS for K=$k id $cost%2.2f")
    }
//通过交叉验证选择K(用户的)
val trainTestSplitUsers=userVectors.randomSplit(Array(0.6,0.4),123)
    val trainUsers=trainTestSplitMovies(0)
    val testUsers=trainTestSplitMovies(1)
    val costsUsers=Seq(2,3,4,5,10,20).map{
      k=>(k,KMeans.train(trainUsers,numIterations,k,numRuns).computeCost(testUsers))
    }
    println("Movie clustering cross-validation:")
    costsUsers.foreach{
      case (k,cost)=>println(f"WCSS for K=$k id $cost%2.2f")
    }
  }
}
