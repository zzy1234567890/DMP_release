package com.utils

import com.Tags.TagLocation
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月24日
  *
  * @author 张振宇
  * @version ：1.0
  */
object test {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

//    val text: RDD[String] = spark.sparkContext.parallelize(Array("116.310003,39.991957"))
//
//    val res: RDD[String] = text.map(line => {
//      val arr: Array[String] = line.split(",")
//
//      val long = arr(0).toDouble
//
//      val lat = arr(1).toDouble
//
//      AmapUtil.getBusinessFromAmap(long, lat)
//
//    })


    val df: DataFrame = spark.read.parquet("E://result-dmp1")
//
//    val res: RDD[List[(String, Int)]] = df.rdd.map(row => {
//      TagLocation.makeTags(row)
//    }).filter(_.size != 0)


    val res: RDD[String] = df.rdd.map(row => {

      row.getAs[String]("`long`")
    })


//    res.foreach(println)

    println(res.collect().toBuffer)


    spark.stop()

  }
}
