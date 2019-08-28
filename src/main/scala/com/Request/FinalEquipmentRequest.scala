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
object FinalEquipmentRequest {
  def main(args: Array[String]): Unit = {

      if(args.length!=5){
        println("目录参数不正确，退出程序")
        sys.exit()
      }

      //    val Array(inputPath)=args

      val Array(inputPath,outputPath1,outputPath2,outputPath3,outputPath4)=args


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

      //运营商
      val ispname: String =  if(row.getAs[String]("ispname").equals("电信")){
        "电信"
      } else if (row.getAs[String]("ispname").equals("联通")){
        "联通"
      } else if (row.getAs[String]("ispname").equals("移动")){
        "移动"
      } else {
        "其他"
      }

      val reqList: List[Double] = RequestUtils.request(requestmode, processnode)

      val clickList: List[Double] = RequestUtils.click(requestmode, iseffective)


      val costList: List[Double] = RequestUtils.cost(iseffective, isbilling, iswin, adorderid, winprice, adpayment)

      val combineList: List[Double] = reqList ++ clickList ++ costList

      (ispname, combineList)

    }).reduceByKey((x, y) => x.zip(y).map(t => t._1 + t._2))


    val res: RDD[(String, List[Any])] = tup.map(t => {
      val ispname = t._1
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
      (ispname, list1)
    })

    res.saveAsTextFile(outputPath1)
   //--------------------------------------------------------------------------------------------------------

    val tup1: RDD[(String, List[Double])] = df.rdd.map(row => {
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


      val networkmannername: String = if(row.getAs[String]("networkmannername").equals("2G")){
        "2G"
      } else if(row.getAs[String]("networkmannername").equals("3G")){
        "3G"
      } else if(row.getAs[String]("networkmannername").equals("4G")){
        "4G"
      } else if(row.getAs[String]("networkmannername").equals("WiFi")){
        "WiFi"
      } else {
        "其他"
      }

      val reqList: List[Double] = RequestUtils.request(requestmode, processnode)

      val clickList: List[Double] = RequestUtils.click(requestmode, iseffective)


      val costList: List[Double] = RequestUtils.cost(iseffective, isbilling, iswin, adorderid, winprice, adpayment)

      val combineList: List[Double] = reqList ++ clickList ++ costList

      (networkmannername, combineList)

    }).reduceByKey((x, y) => x.zip(y).map(t => t._1 + t._2))


    val res1: RDD[(String, List[Any])] = tup1.map(t => {
      val networkmannername = t._1
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
      (networkmannername, list1)
    })

    res1.saveAsTextFile(outputPath2)
//--------------------------------------------------------------------------------------------------------------------------------
   val tup2: RDD[(String, List[Double])] = df.rdd.map(row => {
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


  val devicetype: String = if(row.getAs[String]("devicetype").equals("手机")){
    "手机"
  } else if(row.getAs[String]("devicetype").equals("平板")){
    "平板"
  } else {
    "其他"
  }

  val reqList: List[Double] = RequestUtils.request(requestmode, processnode)

  val clickList: List[Double] = RequestUtils.click(requestmode, iseffective)


  val costList: List[Double] = RequestUtils.cost(iseffective, isbilling, iswin, adorderid, winprice, adpayment)

  val combineList: List[Double] = reqList ++ clickList ++ costList

  (devicetype, combineList)

}).reduceByKey((x, y) => x.zip(y).map(t => t._1 + t._2))

    val res2: RDD[(String, List[Any])] = tup2.map(t => {
      val devicetype = t._1
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
      (devicetype, list1)
    })

    res2.saveAsTextFile(outputPath3)
   // --------------------------------------------------------------------------------------------------------------------------

    val tup3: RDD[(String, List[Double])] = df.rdd.map(row => {
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


      val os: String = if (row.getAs[Int]("client") == 1) {
        "Andriond"
      } else if (row.getAs[Int]("client") == 2) {
        "IOS"
      } else {
        "其他"
      }

      val reqList: List[Double] = RequestUtils.request(requestmode, processnode)

      val clickList: List[Double] = RequestUtils.click(requestmode, iseffective)


      val costList: List[Double] = RequestUtils.cost(iseffective, isbilling, iswin, adorderid, winprice, adpayment)

      val combineList: List[Double] = reqList ++ clickList ++ costList

      (os, combineList)

    }).reduceByKey((x, y) => x.zip(y).map(t => t._1 + t._2))


    val res3: RDD[(String, List[Any])] = tup3.map(t => {
      val os = t._1
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
      (os, list1)
    })

    res3.saveAsTextFile(outputPath4)




    spark.stop()
  }
}
