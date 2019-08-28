package com.ETL

import com.utils.Utils2Type
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
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
object txt2ParquetCaseClass {
  def main(args: Array[String]): Unit = {
    //判断路径是否正确
    if(args.length!=2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }

    val Array(inputPath,outputPath)=args

    val conf=new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")



    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")


    val lines= spark.sparkContext.textFile(inputPath)


    val logsRDD: RDD[logs] = lines.map(x => x.split(",", -1)).filter(_.length >= 85)
      .map(arr => logs(
        arr(0),
        Utils2Type.toInt(arr(1)),
        Utils2Type.toInt(arr(2)),
        Utils2Type.toInt(arr(3)),
        Utils2Type.toInt(arr(4)),
        arr(5),
        arr(6),
        Utils2Type.toInt(arr(7)),
        Utils2Type.toInt(arr(8)),
        Utils2Type.toDouble(arr(9)),
        Utils2Type.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        Utils2Type.toInt(arr(17)),
        arr(18),
        arr(19),
        Utils2Type.toInt(arr(20)),
        Utils2Type.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        Utils2Type.toInt(arr(26)),
        arr(27),
        Utils2Type.toInt(arr(28)),
        arr(29),
        Utils2Type.toInt(arr(30)),
        Utils2Type.toInt(arr(31)),
        Utils2Type.toInt(arr(32)),
        arr(33),
        Utils2Type.toInt(arr(34)),
        Utils2Type.toInt(arr(35)),
        Utils2Type.toInt(arr(36)),
        arr(37),
        Utils2Type.toInt(arr(38)),
        Utils2Type.toInt(arr(39)),
        Utils2Type.toDouble(arr(40)),
        Utils2Type.toDouble(arr(41)),
        Utils2Type.toInt(arr(42)),
        arr(43),
        Utils2Type.toDouble(arr(44)),
        Utils2Type.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        Utils2Type.toInt(arr(57)),
        Utils2Type.toDouble(arr(58)),
        Utils2Type.toInt(arr(59)),
        Utils2Type.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        Utils2Type.toInt(arr(73)),
        Utils2Type.toDouble(arr(74)),
        Utils2Type.toDouble(arr(75)),
        Utils2Type.toDouble(arr(76)),
        Utils2Type.toDouble(arr(77)),
        Utils2Type.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        Utils2Type.toInt(arr(84))
      ))
    import spark.implicits._

    val df: DataFrame = logsRDD.toDF()

    df.write.parquet(outputPath)


    spark.stop()

  }
}
case class logs(sessionid: String,
                advertisersid: Int,
                adorderid: Int,
                adcreativeid: Int,
                adplatformproviderid: Int,
                sdkversion: String,
                adplatformkey: String,
                putinmodeltype: Int,
                requestmode: Int,
                adprice: Double,
                adppprice: Double,
                requestdate: String,
                ip: String,
                appid: String,
                appname: String,
                uuid: String,
                device: String,
                client: Int,
                osversion: String,
                density: String,
                pw: Int,
                ph: Int,
                longitude: String,
                lat: String,
                provincename: String,
                cityname: String,
                ispid: Int,
                ispname: String,
                networkmannerid: Int,
                networkmannername:String,
                iseffective: Int,
                isbilling: Int,
                adspacetype: Int,
                adspacetypename: String,
                devicetype: Int,
                processnode: Int,
                apptype: Int,
                district: String,
                paymode: Int,
                isbid: Int,
                bidprice: Double,
                winprice: Double,
                iswin: Int,
                cur: String,
                rate: Double,
                cnywinprice: Double,
                imei: String,
                mac: String,
                idfa: String,
                openudid: String,
                androidid: String,
                rtbprovince: String,
                rtbcity: String,
                rtbdistrict: String,
                rtbstreet: String,
                storeurl: String,
                realip: String,
                isqualityapp: Int,
                bidfloor: Double,
                aw: Int,
                ah: Int,
                imeimd5: String,
                macmd5: String,
                idfamd5: String,
                openudidmd5: String,
                androididmd5: String,
                imeisha1: String,
                macsha1: String,
                idfasha1: String,
                openudidsha1: String,
                androididsha1: String,
                uuidunknow: String,
                userid: String,
                iptype: Int,
                initbidprice: Double,
                adpayment: Double,
                agentrate: Double,
                lomarkrate: Double,
                adxrate: Double,
                title: String,
                keywords: String,
                tagid: String,
                callbackdate: String,
                channelid: String,
                mediatype: Int) extends Product