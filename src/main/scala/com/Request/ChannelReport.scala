package com.Request

import com.utils.RequestUtils
import org.apache.spark.rdd.RDD
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
object ChannelReport {
  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }

//        val Array(inputPath)=args

    val Array(inputPath,outputPath)=args

    val conf=new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")



    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val df=spark.read.parquet(inputPath)

    val tup: RDD[(String, List[Double])] = df.rdd.map(row => {
      //将需要的字段全部取到
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")


      val channelid: String = row.getAs[String]("channelid")


      val reqList: List[Double] = RequestUtils.request(requestmode, processnode)

      val clickList: List[Double] = RequestUtils.click(requestmode, iseffective)

      val costList: List[Double] = RequestUtils.cost(iseffective, isbilling, iswin, adorderid, winprice, adpayment)

      val combineList: List[Double] = reqList ++ clickList ++ costList

      (channelid, combineList)
    }).reduceByKey((x, y) => x.zip(y).map(t => t._1 + t._2))



    val res: RDD[(String, List[Any])] = tup.map(t => {
      val channelid = t._1
      val list = t._2
      val initreqno = list(0)
      val effreqno = list(1)
      val adreqno = list(2)
      val partbidno = list(3)
      val partsucceededno = list(4)
      val exhibitionno = list(5)
      val clickno = list(6)
      val CTR = if (exhibitionno!=0.0) (clickno / exhibitionno * 100.0).toString + "%" else "0.0%"
      val dspadconsume = list(7)
      val dspadcost = list(8)

      val list1 = List(initreqno, effreqno, adreqno, partbidno, partsucceededno, exhibitionno, clickno, CTR, dspadconsume, dspadcost)
      (channelid, list1)
    })

//    println(res.collect().toBuffer)

    res.saveAsTextFile(outputPath)

    spark.stop()


  }
}
