package com.Request

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import com.typesafe.config.{Config, ConfigFactory}
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

//地域分布指标
object LocationRequest {

  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }

//    val Array(inputPath)=args

    val Array(inputPath,outputPath)=args

    val conf=new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")


    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val df=spark.read.parquet(inputPath)


    val res: RDD[((String, String), List[Double])] = df.rdd.map(row => {
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

      //省、市
      val provincename: String = row.getAs[String]("provincename")

      val cityname: String = row.getAs[String]("cityname")

      val reqList: List[Double] = RequestUtils.request(requestmode, processnode)

      val clickList: List[Double] = RequestUtils.click(requestmode, iseffective)

      val costList: List[Double] = RequestUtils.cost(iseffective, isbilling, iswin, adorderid, winprice, adpayment)

      val combineList: List[Double] = reqList ++ clickList ++ costList

      ((provincename, cityname), combineList)
    }).reduceByKey((x, y) => x.zip(y).map(t => t._1 + t._2))


    res.foreachPartition(x=>data2MaSQL(x))

   // println(res.map(tup => tup._1._1 + "-" + tup._1._2 + "," + tup._2.mkString(",")).collect().toBuffer)


    res.saveAsTextFile(outputPath)



    spark.stop()
  }

  val data2MaSQL=(it:Iterator[((String, String), List[Double])])=>{

    val load: Config = ConfigFactory.load()

    var conn:Connection=null

    var ps:PreparedStatement=null

    val sql="insert into areatable(provincename,cityname,initreqno,effreqno,adreqno,partbidno,partsucceededno,exhibitionno,clickno,dspadconsume,dspadcost) values(?,?,?,?,?,?,?,?,?,?,?)"

    val jdbcUrl=load.getString("jdbc.url")

    val user=load.getString("jdbc.user")

    val password=load.getString("jdbc.password")


    try {
      conn = DriverManager.getConnection(jdbcUrl, user, password)

      it.foreach(tup => {

        ps = conn.prepareStatement(sql)

        ps.setString(1, tup._1._1)

        ps.setString(2, tup._1._2)

        ps.setDouble(3,tup._2(0))

        ps.setDouble(4,tup._2(1))

        ps.setDouble(5,tup._2(2))

        ps.setDouble(6,tup._2(3))

        ps.setDouble(7,tup._2(4))

        ps.setDouble(8,tup._2(5))

        ps.setDouble(9,tup._2(6))

        ps.setDouble(10,tup._2(7))

        ps.setDouble(11,tup._2(8))


        ps.executeUpdate()
      })
    } catch {
      case e:Exception =>println(e.printStackTrace())
    } finally {
      if(ps!=null)
        ps.close()
      if (conn!=null)
        conn.close()
    }
  }


}
