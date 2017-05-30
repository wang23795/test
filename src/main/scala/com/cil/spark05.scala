package com.cil

import breeze.linalg.{SparseVector, norm}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
/**
  * Created by Administrator on 2017/5/19.
  */
object spark05 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf=new SparkConf().setAppName("k-mean").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val path="F://a//d/*"
    val rdd=sc.wholeTextFiles(path)
    val text=rdd.map{case(file,text)=>text}
    //println(text.count())
    //val newsgroups=rdd.map{case(file,text)=>file.split("/").takeRight(2).head}
    //val countByGroup=newsgroups.map(n=>(n,1)).reduceByKey(_+_).collect().sortBy(-_._2).mkString("\n")
    //println(countByGroup.toString)
    //val text1=rdd.map{case(file,text)=>text}
    //val whiteSpaceSplit=text.flatMap(t=>t.split(" ").map(_.toLowerCase()))
    //println(whiteSpaceSplit.distinct().count())
    //println(whiteSpaceSplit.sample(true,0.3,42).take(100).mkString(","))
    //使用正则表达去掉标点符号
    val nonWordSplit=text.flatMap(t=>t.split("""\W+""").map(_.toLowerCase()))
    //使用正则去掉单词中包含数字的单词
    val regex="""[^0-9]*""".r()
    val filterNumbers=nonWordSplit.filter(token=>regex.pattern.matcher(token).matches())
    //取出频率最高的一些常用词语
    val tokenCounts=filterNumbers.map(t=>(t,1)).reduceByKey(_+_)
    val oreringDesc=Ordering.by[(String,Int),Int](_._2)
    //将频率最高的次放到set里面,将其过滤掉
    val stopwords=Set("the","to","a","of","and")
    val tokenCountsFilteredStopwords=tokenCounts.filter{
      case (k,v)=> !stopwords.contains(k)
    }
    //去除只有一个单词的
    val tokenCountsFiltereSize=tokenCountsFilteredStopwords.filter{
     case (k,v)=> k.size>=2
    }


    //取出出现次数很少的单词
    val oreringAsc=Ordering.by[(String,Int),Int](-_._2)
    val rareTokens=tokenCounts.filter{
      case(k,v)=> v<2
    }.map {
      case (k, v) => k
    }.collect().toSet
    val tokenCountsFilteredAll=tokenCountsFiltereSize.filter{
      case(k,v)=> !rareTokens.contains(k)
    }

    //将整个过滤的函数融合
    def tokenize(line:String):Seq[String]={
      line.split("""\W+""").
        map(_.toLowerCase()).
        filter(token=> regex.pattern.matcher(token).matches()).
        filterNot(token=>stopwords.contains(token)).
        filterNot(token=>rareTokens.contains(token)).
        filter(token=>token.size>=2).
        toSeq
    }
    val tokens=text.map(doc=>tokenize(doc))
    //开始训练啦,hashingTF
    val dim=math.pow(2,18).toInt
    val hashingTF=new HashingTF(dim)
    val tf=hashingTF.transform(tokens)
    val v=tf.first().asInstanceOf[SV]//返回值是向量形式
    val idf=new IDF().fit(tf)
    val tfidf=idf.transform(tf)//转换为tfidf
    val v2=tfidf.first().asInstanceOf[SV]
    val minMaxVals=tfidf.map{
      v=>
        val sv=v.asInstanceOf[SV]
        (sv.values.min,sv.values.max)
    }
    val globalMinMax=minMaxVals.reduce{
      case((min1,max1),(min2,max2))=>
        (math.min(min1,min2),math.max(max1,max2))
    }
    //利用ft-idf计算不同类文本之间的相似度

        val graphicsText=rdd.filter{case (file,text)=> file.contains("graphics")}
        val graphicsTF=graphicsText.mapValues(doc=>hashingTF.transform(tokenize(doc)))
        val graphicsTfIdf=idf.transform(graphicsTF.map(_._2))//使用以前计算好的基于所有样本的idf值
        //随机选取测试样本
        val graphics1=graphicsTfIdf.sample(true,0.1,42).first().asInstanceOf[SV]
        val breeze1=new SparseVector(graphics1.indices,graphics1.values,graphics1.size)

    val windowsText=rdd.filter{case (file,text)=> file.contains("windows")}
    val windowsTF=windowsText.mapValues(doc=>hashingTF.transform(tokenize(doc)))
    val windowsTfIdf=idf.transform(windowsTF.map(_._2))//使用以前计算好的基于所有样本的idf值
    //随机选取测试样本
    val windows1=windowsTfIdf.sample(true,0.1,42).first().asInstanceOf[SV]
    val breeze2=new SparseVector(windows1.indices,windows1.values,windows1.size)

        //计算想死度
        val cosineSim=breeze1.dot(breeze2)/(norm(breeze1)*norm(breeze2))
        println(cosineSim)
//利用tf-idf来计算文本之间的相似度,先计算属于本某一文件夹下样本的tf-idf值
//    val graphicsText=rdd.filter{case (file,text)=> file.contains("graphics")}
//    val graphicsTF=graphicsText.mapValues(doc=>hashingTF.transform(tokenize(doc)))
//    val graphicsTfIdf=idf.transform(graphicsTF.map(_._2))//使用以前计算好的基于所有样本的idf值
//    //随机选取测试样本
//    val graphics1=graphicsTfIdf.sample(true,0.1,42).first().asInstanceOf[SV]
//    val breeze1=new SparseVector(graphics1.indices,graphics1.values,graphics1.size)
//    val graphics2=graphicsTfIdf.sample(true,0.1,43).first().asInstanceOf[SV]
//    val breeze2=new SparseVector(graphics2.indices,graphics2.values,graphics2.size)
//    //计算想死度
//    val cosineSim=breeze1.dot(breeze2)/(norm(breeze1)*norm(breeze2))
//    println(cosineSim)

    //查看几个不常用单词，不常用单词的tfidf的值,算idf之前都要算tf,他们的tfidf的值较小
//        val common=sc.parallelize(Seq(Seq("telescope","legislation","investment")))
//        val tfCommon=hashingTF.transform(common)
//        val tfidfCommon=idf.transform(tfCommon)
//        val commonVector=tfidfCommon.first().asInstanceOf[SV]
//        println(commonVector.values.toSeq.toString())
    //查看几个常用单词，不常用单词的tfidf的值,算idf之前都要算tf,他们的tfidf的值较小
//    val common=sc.parallelize(Seq(Seq("you","do","we")))
//    val tfCommon=hashingTF.transform(common)
//    val tfidfCommon=idf.transform(tfCommon)
//    val commonVector=tfidfCommon.first().asInstanceOf[SV]
//    println(commonVector.values.toSeq.toString())

    //println(globalMinMax)
        //println(v2.size)
//        println(v2.values.size)
//        println(v2.values.take(10).toSeq.toString())
//        println(v2.indices.take(10).toSeq.toString())
//    println(v.size)
//    println(v.values.size)
//    println(v.values.take(10).toSeq.toString())
//    println(v.indices.take(10).toSet.toString())
    //println(tokens.first().take(10).mkString(","))
    //println(text.flatMap(doc=>tokenize(doc)).distinct().count())
    //println(tokenCountsFilteredAll.count())

    //println(tokenCountsFilteredAll.top(20)(oreringAsc).mkString("\n"))
    //println(tokenCountsFilteredStopwords.top(20)(oreringAsc).mkString("\n"))
    //println(tokenCountsFilteredStopwords.top(20)(oreringDesc).mkString("\n"))
    //println(tokenCounts.top(20)(oreringDesc).mkString("\n"))
    //println(filterNumbers.distinct().sample(true,0.3,42).take(100).mkString(","))
    //println(nonWordSplit.distinct().count())
    //println(nonWordSplit.distinct().sample(true,0.3,42).take(100).mkString(","))
  }
}
