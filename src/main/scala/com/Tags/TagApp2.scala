package com.Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
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
object TagApp2 extends  Tag{

  override def makeTags(args: Any*): List[(String, Int)] = {

    var list=List[(String,Int)]()

    val row: Row = args(0).asInstanceOf[Row]


    val appBroadcastInfo= args(1).asInstanceOf[Jedis]


    val appId: String = row.getAs[String]("appid")


    val appName: String = row.getAs[String]("appname")

    if(StringUtils.isNotBlank(appName)){
      list:+=("APP"+appName,1)
    } else {
      if(StringUtils.isNotBlank(appId)){
        list:+=("APP"+appBroadcastInfo.get(appId),1)
      }
    }

    list

  }
}

