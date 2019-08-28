package com.utils

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月24日
  *
  * @author 张振宇
  * @version ：1.0
  */
object APP2Jedis {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")



    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val dict: RDD[String] = spark.sparkContext.textFile("E://sparkNewStage/test/dataP/app_dict.txt")


      dict.map(_.split("\\s+", -1))
      .filter(_.length >= 5).foreachPartition(arr=>{

        val jedis=JedisConnectionPool.getConnection()
          arr.foreach(word=>{

          jedis.set(word(4),word(1))
        })

        jedis.close()

      })

    spark.stop()

  }
}
