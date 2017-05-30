package com.cil


import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.optimization.{SimpleUpdater, SquaredL2Updater, Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.{Entropy, Gini, Impurity}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/5/18.
  */
object spark03 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf=new SparkConf().setAppName("classifity").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val rawData=sc.textFile("F:/a/kaggle")
    val records=rawData.filter(m=>m.startsWith("\"http")).map(line=>line.split("\t"))
    val data=records.map{
      r=>
         val trimmed=r.map(_.replace("\"",""))
         val label=trimmed(r.size-1).toInt
         val feature=trimmed.slice(4,r.size-1).map(d=>if (d=="?") 0.0 else d.toDouble)
        LabeledPoint(label,Vectors.dense(feature))
    }
    val nbData=records.map{
      r=>
        val trimmed=r.map(_.replace("\"",""))
        val label=trimmed(r.size-1).toInt
        val feature=trimmed.slice(4,r.size-1).map(d=>if (d=="?") 0.0 else d.toDouble).map(d=>if(d<0)0.0 else d)
        LabeledPoint(label,Vectors.dense(feature))
    }
    data.cache()
    //训练模型
//    val lrModel=LogisticRegressionWithSGD.train(data,10)
//    val svmModel=SVMWithSGD.train(data,10)
//    val dtModel=DecisionTree.train(data,Algo.Classification,Entropy,5)
//    val nbModel=NaiveBayes.train(nbData)

//用模型预测准确率
//    val lrTotalCorrect=data.map{
//      point=>
//        if(lrModel.predict(point.features)==point.label) 1 else 0
//    }.sum()
//    val lrAccuracy=lrTotalCorrect/data.count()
//    println(f"lr accuracy is : $lrAccuracy")
//
//    val svmTotalCorrect=data.map{
//      point=>
//        if(svmModel.predict(point.features)==point.label) 1 else 0
//    }.sum()
//    val svmAccuracy=svmTotalCorrect/data.count()
//    println(f"svm accuracy is : $svmAccuracy")
//
//    val nbTotalCorrect=data.map{
//      point=>
//        if(nbModel.predict(point.features)==point.label) 1 else 0
//    }.sum()
//    val nbAccuracy=nbTotalCorrect/data.count()
//    println(f"nb accuracy is : $nbAccuracy")
//
//
//    val dtTotalCorrect=data.map{
//      point=>
//        val score=dtModel.predict(point.features)
//        val predict=if(score>0.5)1 else 0
//        if(predict==point.label)1 else 0
//    }.sum()
//    val dtAccuracy=dtTotalCorrect/data.count()
//    println(f"dt accuracy is : $dtAccuracy")

    //Roc相关，计算ACU
//    val lrMetrics=Seq(lrModel,svmModel).map{
//      model=>
//        val scoreAndLabels=data.map{
//          point=>(model.predict(point.features),point.label)
//        }
//        val lrMetrics=new BinaryClassificationMetrics(scoreAndLabels)
//        (model.getClass.getSimpleName,lrMetrics.areaUnderPR(),lrMetrics.areaUnderROC())
//    }
//
//    val nbMetrics=Seq(nbModel).map{
//      model=>
//        val scoreAndLabels=nbData.map{
//          point=>
//            val score=model.predict(point.features)
//            (if(score>0.5)1.0 else 0.0,point.label)
//        }
//        val nbMetrics=new BinaryClassificationMetrics(scoreAndLabels)
//        (model.getClass.getSimpleName,nbMetrics.areaUnderPR(),nbMetrics.areaUnderROC())
//    }
//
//    val dtMetrics=Seq(dtModel).map{
//      model=>
//        val scoreAndLabels=data.map{
//          point=>
//            val score=model.predict(point.features)
//            (if(score>0.5)1.0 else 0.0,point.label)
//        }
//        val metrics=new BinaryClassificationMetrics(scoreAndLabels)
//        (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC())
//    }
//    val allMetrics=lrMetrics++nbMetrics++dtMetrics
//    allMetrics.foreach{
//      case(m,pr,roc)=>
//        println(f"$m,Area under PR:${pr*100.0}%2.4f%%,Area under ROC:${roc*100.0}%2.4f%%")
//    }
    //val dataPoint=data.first()
//  //特征标准化
//    val vectors=data.map(lp=>lp.features)
//    val matrix=new RowMatrix(vectors)
//    //由此可以计算出方差均值等
//    val matrixSummary=matrix.computeColumnSummaryStatistics()
//    //println(matrixSummary.mean)
//    val scaler=new StandardScaler(withMean =true,withStd = true).fit(vectors)
//    val scaledData=data.map(lp=>LabeledPoint(lp.label,scaler.transform(lp.features)))
    //println(data.first().features)
    //println(scaledData.first().features)

    //标准化后的逻辑线性回归
//    val lrModelScaled=LogisticRegressionWithSGD.train(scaledData,10)
//        val lrTotalCorrect=scaledData.map{
//          point=>
//            if(lrModelScaled.predict(point.features)==point.label) 1 else 0
//        }.sum()
//        val lrAccuracy=lrTotalCorrect/scaledData.count()
//        println(f"lr accuracy is : $lrAccuracy")
//        val lrMetricsScaled=Seq(lrModelScaled).map{
//          model=>
//            val scoreAndLabels=scaledData.map{
//              point=>(model.predict(point.features),point.label)
//            }
//            val lrMetrics=new BinaryClassificationMetrics(scoreAndLabels)
//            (model.getClass.getSimpleName,lrMetrics.areaUnderPR(),lrMetrics.areaUnderROC())
//        }
//        val allMetrics=lrMetricsScaled
//        allMetrics.foreach{
//          case(m,pr,roc)=>
//            println(f"$m,Area under PR:${pr*100.0}%2.4f%%,Area under ROC:${roc*100.0}%2.4f%%")
//        }
//将类别特征考虑进来
    val categories=records.map(r=>r(3)).distinct().collect().zipWithIndex.toMap
    val numCategories=categories.size
    val dataCategories=records.map{
      r=>
        val trimmed=r.map(_.replaceAll("\"",""))
        val label=trimmed(r.size-1).toInt
        val categoryIdx=categories(r(3))
        val categoryFeatures=Array.ofDim[Double](numCategories)
        categoryFeatures(categoryIdx)=1.0
        val otherFeatures=trimmed.slice(4,r.size-1).map(d=>if(d=="?")0.0 else d.toDouble)
        val features=categoryFeatures++otherFeatures
        LabeledPoint(label,Vectors.dense(features))
    }
    val scalerCats=new StandardScaler(withMean = true,withStd = true).fit(dataCategories.map(lp=>lp.features))
    val scaledDataCats=dataCategories.map(lp=>
    LabeledPoint(lp.label,scalerCats.transform(lp.features)))
  //  println(scaledDataCats.first().toString())
    //类别特征考虑后的评估
//        val lrModelScaledAndCat=LogisticRegressionWithSGD.train(scaledDataCats,10)
//            val lrTotalCorrect=scaledDataCats.map{
//              point=>
//                if(lrModelScaledAndCat.predict(point.features)==point.label) 1 else 0
//            }.sum()
//            val lrAccuracy=lrTotalCorrect/scaledDataCats.count()
//            println(f"lr accuracy is : $lrAccuracy")
//            val lrMetricsScaled=Seq(lrModelScaledAndCat).map{
//              model=>
//                val scoreAndLabels=scaledDataCats.map{
//                  point=>(model.predict(point.features),point.label)
//                }
//                val lrMetrics=new BinaryClassificationMetrics(scoreAndLabels)
//                (model.getClass.getSimpleName,lrMetrics.areaUnderPR(),lrMetrics.areaUnderROC())
//            }
//            val allMetrics=lrMetricsScaled
//            allMetrics.foreach{
//              case(m,pr,roc)=>
//                println(f"$m,Area under PR:${pr*100.0}%2.4f%%,Area under ROC:${roc*100.0}%2.4f%%")
//            }
    //不同迭代次数对模型效果的影响
    def trainWithParams(input:RDD[LabeledPoint],regParam:Double,numIterations:Int,updater:Updater,stepSize:Double)={

  var lr=new LogisticRegressionWithSGD
  lr.optimizer.setNumIterations(numIterations).setUpdater(updater).setRegParam(regParam).setStepSize(stepSize)
  lr.run(input)
}
    def createMetrics(label:String,data:RDD[LabeledPoint],model:ClassificationModel) ={
      val scoreAndLabels=data.map{
        point=>(model.predict(point.features),point.label)
      }
      val metrics=new BinaryClassificationMetrics(scoreAndLabels)
      (label,metrics.areaUnderROC())
    }
    //不同步长对结果的影响
//    val iterResults=Seq(1,5,10,50).map{
//      param=>
//        val model=trainWithParams(scaledDataCats,0.0,param,new SimpleUpdater,1.0)
//        createMetrics(s"$param iterations",scaledDataCats,model)
//    }
//    iterResults.foreach{ case (param,auc)=>println(f"$param,AUC=${auc*100}%2.2f%%")
//    }
//    val stepResults=Seq(0.001,0.01,0.1,1.0,10.0).map{
//      param=>
//        val model=trainWithParams(scaledDataCats,0.0,10,new SimpleUpdater,param)
//      createMetrics(s"$param step size",scaledDataCats,model)
//    }
//    stepResults.foreach{
//      case(param,auc)=>println(f"$param,AUC=${auc*100}%2.2f%%")
//    }
    //不同正则化参数对结果的影响
//    val regResults=Seq(0.001,0.01,0.1,1.0,10.0).map{
//  param=>
//    val model=trainWithParams(scaledDataCats,param,10,new SquaredL2Updater,1.0)
//    createMetrics(s"$param L2 regularization parameter",scaledDataCats,model)
//}
//    regResults.foreach{
//
//      case(param,auc)=>println(f"$param,AUC=${auc*100}%2.2f%%")
//    }
    //决策树的深度跟不纯度的调优
//    def trainDTWithParams(input:RDD[LabeledPoint],maxDepth:Int,impurity:Impurity) ={
//  DecisionTree.train(input,Algo.Classification,impurity,maxDepth)
//}
//    val dtResultsEntropy=Seq(1,2,3,4,5,10,20).map{
//      param=>
//        val model=trainDTWithParams(data,param,Gini)
//        val scoreAndLabels=data.map{
//          point=>
//          val score=model.predict(point.features)
//            (if (score>0.5)1.0 else 0.0,point.label)
//        }
//        val metrics=new BinaryClassificationMetrics(scoreAndLabels)
//        (s"$param tree depth",metrics.areaUnderROC())
//    }
//    dtResultsEntropy.foreach{case(param,auc)=>println(f"$param,AUC=${auc*100}%2.2f%%")}
    //交叉验证
// val trainTestSplit=scaledDataCats.randomSplit(Array(0.6,0.4),123)
//    val train=trainTestSplit(0)
//    val test=trainTestSplit(1)
//    val regResultsTest=Seq(0.0,0.001,0.0025,0.005,0.01).map{
//      param=>
//        val model=trainWithParams(train,param,10,new SquaredL2Updater,1.0)
//        createMetrics(s"$param L2 regularization parameter",test,model)
//    }
//    regResultsTest.foreach{
//      case(param,auc)=>println(f"$param,AUC=${auc*100}%2.6f%%")
//    }
     // println(dataCategories.first())
//    println(categories.toString())
//    println(numCategories)
    //println(f"prediction value is $prediction")
    //println(f"true value is $trueLabel")
    //println(rawData.filter(m=>m.startsWith("\"http")).count())
    //println(records.first().mkString("\n"))


  }
}
