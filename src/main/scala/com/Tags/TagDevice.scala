package com.Tags

import org.apache.spark.sql.Row

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月23日
  *
  * @author 张振宇
  * @version ：1.0
  */
object TagDevice extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()

    val row: Row = args(0).asInstanceOf[Row]

    val osVersion: Int = row.getAs[Int]("client")

    osVersion match {
//      case  v if (v >=1 && v <=3 )=>list :+ ("操作系统",1)

      case 1 =>list :+=("Android D00010001",1)
      case 2 =>list :+=("IOS D00010002",1)
      case 3 =>list :+=("WinPhone D00010003",1)
      case _ =>list :+=("其 他 D00010004",1)
    }






    val networkMannerName: String = row.getAs[String]("networkmannername")


    networkMannerName
    match {
//      case v if(v.equals("WiFi")|| v.equals("4G")||v.equals("3G")||v.equals("2G")) =>list :+("联网方",1)

//      case v if(v.equals("WiFi"))  =>list :+("D00020001",1)
//      case v if(v.equals("4G"))  =>list :+("D00020002",1)
//      case v if(v.equals("3G"))  =>list :+("D00020003",1)
//      case v if(v.equals("2G"))  =>list :+("D00020004",1)

      case "WiFi" =>list :+=("D00020001",1)
      case "4G"  =>list :+=("D00020002",1)
      case "3G" =>list :+=("D00020003",1)
      case "2G" =>list :+=("D00020004",1)
      case _  =>list :+=("D00020005",1)
    }




    val isPname: String = row.getAs[String]("ispname")

    isPname match {

//      case v if(v.equals("移动")|| v.equals("联通")||v.equals("电信")) => list :+("运营商",1)

      case "移动"=>list :+("D00030001",1)
      case "联通"=>list :+("D00030002",1)
      case "电信"=>list :+("D00030003",1)
      case _=>list :+("D00030004",1)
    }



    list
  }
}
