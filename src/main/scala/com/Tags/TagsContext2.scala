package com.Tags

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月24日
  *
  * @author 张振宇
  * @version ：1.0
  */
object TagsContext2 {
  def main(args: Array[String]): Unit = {




    if(args.length!=4){
      println("目录参数不正确，退出程序")
      sys.exit()
    }

    val Array(inputPath,outputPath,dirPath,stopPath)=args

    //        if(args.length!=1){
    //          println("目录参数不正确，退出程序")
    //          sys.exit()
    //        }
    //
    //    val Array(inputPath)=args

    val conf=new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")



    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    //仅限于测试环境
    val map: collection.Map[String, String] = spark.sparkContext.textFile("E://sparkNewStage/test/dataP/app_dict.txt")
      .map(_.split("\\s+", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()



    val broadcastInfo: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(map)


    val stopWordsMap: collection.Map[String, Int] = spark.sparkContext.textFile(stopPath).map((_,0)).collectAsMap()

    //    val stopWordsMap: collection.Map[String, Int] = spark.sparkContext.textFile("E://sparkNewStage/test/dataP/stopwords.txt").map((_,0)).collectAsMap()


    val broadcastStopWords: Broadcast[collection.Map[String, Int]] = spark.sparkContext.broadcast(stopWordsMap)

    val df=spark.read.parquet(inputPath)

    df.filter(TagUtils.uniqueUserId).rdd
      .mapPartitions(row=>{
        val jedis: Jedis = JedisConnectionPool.getConnection()

        var list=ListBuffer[(String,List[(String,Int)])]()
        row.map(row=>{
          val userId=TagUtils.getUniqueUserId(row)

          //通过row数据按照需求打上所有标签

          val adList=TagsAd.makeTags(row)

          val appList: List[(String, Int)] = TagApp.makeTags(row,jedis)


          val channelList: List[(String, Int)] = TagChannel.makeTags(row)

          val deviceList: List[(String, Int)] = TagDevice.makeTags(row)

          val keyWordList: List[(String, Int)] = TagKeyWord.makeTags(row,broadcastStopWords)

          val areaList: List[(String, Int)] = TagArea.makeTags(row)

          list.append((userId,adList++appList++channelList++deviceList++keyWordList++areaList))
        })

        jedis.close()

        list.toIterator
      })
      .reduceByKey((x,y)=>
        (x:::y).groupBy(_._1).mapValues(_.foldLeft[Int](0)((x,y)=>x+y._2)).toList
      ).foreach(println)

    spark.stop()
  }
}
