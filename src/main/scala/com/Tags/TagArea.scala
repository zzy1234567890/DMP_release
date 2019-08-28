package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
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
object TagArea extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()

    val row: Row = args(0).asInstanceOf[Row]



    val provinceName: String = row.getAs[String]("provincename")

    if(StringUtils.isNotBlank(provinceName)){
      list :+= ("ZP"+provinceName,1)
    }





    val cityName: String = row.getAs[String]("cityname")

    if(StringUtils.isNotBlank(cityName)){
      list :+=("ZC"+cityName,1)
    }



    list
  }
}
