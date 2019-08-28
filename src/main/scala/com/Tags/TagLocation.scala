package com.Tags

import ch.hsr.geohash.GeoHash
import com.utils.{AmapUtil, JedisConnectionPool, Tag, Utils2Type}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月26日
  *
  * @author 张振宇
  * @version ：1.0
  */
object TagLocation extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()

    val row: Row = args(0).asInstanceOf[Row]



    val long: String = row.getAs[String]("`long`")

    val lat: String = row.getAs[String]("lat")

    if(  Utils2Type.toDouble(long)  >=73.0 && Utils2Type.toDouble(long)<=135.0
      && Utils2Type.toDouble(lat)>=3.0 && Utils2Type.toDouble(lat)<=54.0){


//      list :+= (AmapUtil.getBusinessFromAmap(long.toDouble,lat.toDouble),1)

      val business: String = getBusiness(long.toDouble,lat.toDouble)

      if (StringUtils.isNotBlank(business)){

//        val business: String = AmapUtil.getBusinessFromAmap(long.toDouble,lat.toDouble)

        val lines: Array[String] = business.split(",")

        lines.foreach(word=>list :+(word,1))

      }
    }
    list
  }

  def getBusiness(long:Double,lat:Double):String={

    //转换
    val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)

    val business: String = redisQueryBusiness(geohash)


    if (business==null || business.length==0){

      val business: String = AmapUtil.getBusinessFromAmap(long,lat)


      //如果调用高德地图解析商圈，那么需要将此次商圈存入redis

      redisInsertBusiness(geohash,business)
    }
      business
  }


  //获取商圈信息

  def redisQueryBusiness(geohash:String):String={
    val jedis: Jedis = JedisConnectionPool.getConnection()

    val business: String = jedis.get(geohash)

          jedis.close()

      business
  }




  def redisInsertBusiness(geoHash:String,business:String)={

    val jedis: Jedis = JedisConnectionPool.getConnection()

    jedis.set(geoHash,business)

    jedis.close()

  }



}
