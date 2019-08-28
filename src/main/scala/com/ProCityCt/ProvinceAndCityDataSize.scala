package com.ProCityCt

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月21日
  *
  * @author 张振宇
  * @version ：1.0
  */
object ProvinceAndCityDataSize {
  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }

    val Array(inputPath,outputPath)=args

    val conf=new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")


    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    val df=spark.read.parquet(inputPath)

    df.createOrReplaceTempView("dmpTable")

    val res=spark.sql("select count(1) cnt,provincename,cityname from dmpTable group by provincename,cityname")


    res.coalesce(1).write.partitionBy("provincename","cityname").json(outputPath)


    val load: Config = ConfigFactory.load()

    val prop=new Properties()

    prop.setProperty("user",load.getString("jdbc.user"))

    prop.setProperty("password",load.getString("jdbc.password"))


    res.write.mode(SaveMode.Append)jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop)




    spark.stop()

  }
}
