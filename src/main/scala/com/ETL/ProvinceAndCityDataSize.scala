package com.ETL

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月20日
  *
  * @author 张振宇
  * @version ：1.0
  */
object ProvinceAndCityDataSize {

  def main(args: Array[String]): Unit = {

//    val conf=new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
//
//    val spark=SparkSession.builder().config(conf).getOrCreate()


    //如果将结果保存在HDFS上需要将配置文件放到资源目录下
    val spark=SparkSession
      .builder()
      .appName(this.getClass.getName)
      .config("spark.sql.warehouse.dir","hdfs://node01:9000/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

//    val df: DataFrame = spark.read.parquet("E://GP22_resultOutput")
//
//    df.createOrReplaceTempView("dmpTable")
//
//    val res=spark.sql("select count(1) cnt,provincename,cityname from dmpTable group by provincename,cityname")
//
//    res.write.json("hdfs://node01:9000/spark-dmpresult")

    val df: DataFrame = spark.read.parquet("hdfs://node01:9000/spark-dmpresult")

    df.createOrReplaceTempView("dmpHDFSTable")

    spark.sql("create table if not exists dmpPartialHDFSTab( cnt String,provincename String,cityname String) partitioned by (provincename String,cityname String)")

    spark.sql("insert into table dmpPartialHDFSTab partition(provincename,cityname) select * from dmpHDFSTable")






    //将结果保存在数据库当中my_visit

//    val prop=new Properties()
//
//    prop.put("user","root")
//
//    prop.put("password","123456")
//
//    val url="jdbc:mysql://node03:3306/my_visit?useUnicode=true&characterEncoding=utf8"
//
//    res.write.mode(SaveMode.Append).jdbc(url,"dmpPartialTab",prop)


    spark.stop()
  }
}
