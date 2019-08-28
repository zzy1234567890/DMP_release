package com.Tags

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月23日
  *
  * @author 张振宇
  * @version ：1.0
  */
object TagsContext {

  def main(args: Array[String]): Unit = {




    if(args.length!=5){
      println("目录参数不正确，退出程序")
      sys.exit()
    }

    val Array(inputPath,outputPath,dirPath,stopPath,date)=args



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

//    val stopWordsMap: collection.Map[String, Int] = spark.sparkContext.textFile(stopPath).map((_,0)).collectAsMap()

    val stopWordsMap: collection.Map[String, Int] = spark.sparkContext.textFile("E://sparkNewStage/test/dataP/stopwords.txt").map((_,0)).collectAsMap()


    val broadcastStopWords: Broadcast[collection.Map[String, Int]] = spark.sparkContext.broadcast(stopWordsMap)

    //todo 调用hbaseAPI
    val load: Config = ConfigFactory.load()

    val hbaseTableName: String = load.getString("hbase.TableName")
    //创建Hadoop任务

    val configuration: Configuration = spark.sparkContext.hadoopConfiguration

//    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))

    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.zookeeper.quorum"))

    configuration.set("hbase.zookeeper.property.clientPort",load.getString("hbase.zookeeper.property.clientPort"))


    val hbConn: Connection = ConnectionFactory.createConnection(configuration)

    val hbAdmin: Admin = hbConn.getAdmin

    if(!hbAdmin.tableExists(TableName.valueOf(hbaseTableName))){

      val tableDescriptor: HTableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))

      val descriptor: HColumnDescriptor = new HColumnDescriptor("tags")

      tableDescriptor.addFamily(descriptor)

      hbAdmin.createTable(tableDescriptor)

      hbAdmin.close()

      hbConn.close()

    }


    val jobConf: JobConf = new JobConf(configuration)

    //指出输出类型和表
    jobConf.setOutputFormat(classOf[TableOutputFormat])


    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)

    val df=spark.read.parquet(inputPath)

    df.filter(TagUtils.uniqueUserId).rdd
      .map(row=>{
      //取出用户id

      val userId=TagUtils.getUniqueUserId(row)

      //通过row数据按照需求打上所有标签

         val adList=TagsAd.makeTags(row)

        val appList: List[(String, Int)] = TagApp.makeTags(row,broadcastInfo)

        val channelList: List[(String, Int)] = TagChannel.makeTags(row)

        val deviceList: List[(String, Int)] = TagDevice.makeTags(row)

        val keyWordList: List[(String, Int)] = TagKeyWord.makeTags(row,broadcastStopWords)

        val areaList: List[(String, Int)] = TagArea.makeTags(row)

        val locationList: List[(String, Int)] = TagLocation.makeTags(row)

//        (userId,adList++appList++channelList++deviceList++keyWordList++areaList++locationList)

        (userId,adList++appList++channelList++deviceList++keyWordList++areaList++locationList)
    })
      .reduceByKey((x,y)=>
        (x:::y).groupBy(_._1)
          .mapValues(_.foldLeft[Int](0)((x,y)=>x+y._2)).toList
    ).map{
      case (userid,userTag)=>{

        val put: Put = new Put(Bytes.toBytes(userid))

        val tags: String = userTag.map(t=>t._1+","+t._2).mkString(",")

        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(date),Bytes.toBytes(tags))

        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(jobConf)  //保存到对应的表中

    spark.stop()
  }
}
