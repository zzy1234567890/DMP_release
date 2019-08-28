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
object TagChannel extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {


    var list=List[(String,Int)]()

    val row: Row = args(0).asInstanceOf[Row]


    val adPlatformProviderId: Int = row.getAs[Int]("adplatformproviderid")

    adPlatformProviderId match {
      case v if v>=0=>list :+=("CN"+adPlatformProviderId,1)
    }


    list
  }
}
