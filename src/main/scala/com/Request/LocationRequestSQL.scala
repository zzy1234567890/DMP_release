package com.Request

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月21日
  *
  * @author 张振宇
  * @version ：1.0
  */
object LocationRequestSQL {

  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }

    val Array(inputPath,outputPath)=args

//    val Array(inputPath)=args

    val conf=new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")



    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val df=spark.read.parquet(inputPath)

    df.createOrReplaceTempView("area")

   val res= spark.sql("select " +
      "provincename,cityname,sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as initreqno," +
      "sum( case when requestmode=1 and processnode>=2 then 1 else 0 end) as effreqno," +
      "sum( case when requestmode=1 and processnode=3 then 1 else 0 end) as adreqno," +
      "sum(case when iseffective=1 and isbilling=1 then 1 else 0 end) as partbidno," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid <>0 then 1 else 0 end) as partsucceededno," +
      "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) as exhibitionno," +
      "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) as clickno," +
     "if(sum(case when requestmode=2 and iseffective=1 then 1 else 0 end)<>0,concat(round((sum(case when requestmode=3 and iseffective=1 then 1 else 0 end)/sum(case when requestmode=2 and iseffective=1 then 1 else 0 end))*100,2),'%'),'0.00%') as CTR," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then winprice else 0.0 end)/1000 as dspadconsume," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment else 0.0 end)/1000 as dspadcost from area " +
      "group by provincename,cityname")

    res.show()

    res.coalesce(1).write.json(outputPath)

    spark.stop()

  }

}
