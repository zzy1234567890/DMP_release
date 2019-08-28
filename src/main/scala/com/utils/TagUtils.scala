package com.utils

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
//标签工具类
object TagUtils {

//过滤所需要的字段

  val uniqueUserId=
    """
      | imei !='' or mac !='' or openudid !='' or androidid !='' or idfa !='' or
      | imeimd5 !='' or macmd5 !='' or openudidmd5 !='' or androididmd5 !='' or idfamd5 !='' or
      | imeisha1 !='' or macsha1 !='' or openudidsha1 !='' or androididsha1 !='' or idfasha1 !='' or
    """.stripMargin

  def getUniqueUserId(row:Row):String={
    row match {
      case  v if StringUtils.isNotBlank(v.getAs[String]("imei"))=>"IM: "+v.getAs[String]("imei")
      case  v if StringUtils.isNotBlank(v.getAs[String]("mac"))=>"MC: "+v.getAs[String]("mac")
      case  v if StringUtils.isNotBlank(v.getAs[String]("openudid"))=>"OUD: "+v.getAs[String]("openudid")
      case  v if StringUtils.isNotBlank(v.getAs[String]("androidid"))=>"ADR: "+v.getAs[String]("androidid")
      case  v if StringUtils.isNotBlank(v.getAs[String]("idfa"))=>"IDF: "+v.getAs[String]("idfa")

      case  v if StringUtils.isNotBlank(v.getAs[String]("imeimd5"))=>"IM5: "+v.getAs[String]("imeimd5")
      case  v if StringUtils.isNotBlank(v.getAs[String]("macmd5"))=>"MC5: "+v.getAs[String]("macmd5")
      case  v if StringUtils.isNotBlank(v.getAs[String]("openudidmd5"))=>"OUD5: "+v.getAs[String]("openudidmd5")
      case  v if StringUtils.isNotBlank(v.getAs[String]("androididmd5"))=>"ADR5: "+v.getAs[String]("androididmd5")
      case  v if StringUtils.isNotBlank(v.getAs[String]("idfamd5"))=>"IDF5: "+v.getAs[String]("idfamd5")

      case  v if StringUtils.isNotBlank(v.getAs[String]("imeisha1"))=>"IMS1: "+v.getAs[String]("imeisha1")
      case  v if StringUtils.isNotBlank(v.getAs[String]("macsha1"))=>"MC1: "+v.getAs[String]("macsha1")
      case  v if StringUtils.isNotBlank(v.getAs[String]("openudidsha1"))=>"OUD1: "+v.getAs[String]("openudidsha1")
      case  v if StringUtils.isNotBlank(v.getAs[String]("androididsha1"))=>"ADR1: "+v.getAs[String]("androididsha1")
      case  v if StringUtils.isNotBlank(v.getAs[String]("idfasha1"))=>"IDF1: "+v.getAs[String]("idfasha1")

    }
  }

  def getAllUserId(row:Row):List[String]={

    var list=List[String]()

    if (StringUtils.isNotBlank(row.getAs[String]("imei"))) list:+= "IM: "+row.getAs[String]("imei")
    if (StringUtils.isNotBlank(row.getAs[String]("mac"))) list:+= "MC: "+row.getAs[String]("mac")
    if (StringUtils.isNotBlank(row.getAs[String]("openudid"))) list:+= "OUD: "+row.getAs[String]("openudid")
    if (StringUtils.isNotBlank(row.getAs[String]("androidid"))) list:+= "ADR: "+row.getAs[String]("androidid")
    if (StringUtils.isNotBlank(row.getAs[String]("idfa"))) list:+= "IDF: "+row.getAs[String]("idfa")
    if (StringUtils.isNotBlank(row.getAs[String]("imeimd5"))) list:+= "IM5: "+row.getAs[String]("imeimd5")
    if (StringUtils.isNotBlank(row.getAs[String]("macmd5"))) list:+= "MC5: "+row.getAs[String]("macmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidmd5"))) list:+= "OUD5: "+row.getAs[String]("openudidmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("androididmd5"))) list:+= "ADR5: "+row.getAs[String]("androididmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("idfamd5"))) list:+= "IDF5: "+row.getAs[String]("idfamd5")
    if (StringUtils.isNotBlank(row.getAs[String]("imeisha1"))) list:+= "IMS1: "+row.getAs[String]("imeisha1")
    if (StringUtils.isNotBlank(row.getAs[String]("macsha1"))) list:+= "MC1: "+row.getAs[String]("macsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidsha1"))) list:+= "OUD1: "+row.getAs[String]("openudidsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("androididsha1"))) list:+= "ADR1: "+row.getAs[String]("androididsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("idfasha1"))) list:+= "IDF1: "+row.getAs[String]("idfasha1")


    list

  }








}
