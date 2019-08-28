package com.utils

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable.ListBuffer

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月24日
  *
  * @author 张振宇
  * @version ：1.0
  */
object AmapUtil {


  def getBusinessFromAmap(long:Double,lat:Double):String={


    val location=long.toString+","+lat.toString

    val urlStr="https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=a459e3fd469b0b704bfa287fc0dfe95f&radius=1000&extensions=all"


    val jsonStr: String = HttpUtil.get(urlStr)

//    val jsonStr="{\"status\":\"1\",\"regeocode\":{\"addressComponent\":{\"city\":[],\"province\":\"北京市\",\"adcode\":\"110108\",\"district\":\"海淀区\",\"towncode\":\"110108015000\",\"streetNumber\":{\"number\":\"5号\",\"location\":\"116.310454,39.9927339\",\"direction\":\"东北\",\"distance\":\"94.5489\",\"street\":\"颐和园路\"},\"country\":\"中国\",\"township\":\"燕园街道\",\"businessAreas\":[{\"location\":\"116.303364,39.97641\",\"name\":\"万泉河\",\"id\":\"110108\"},{\"location\":\"116.314222,39.98249\",\"name\":\"中关村\",\"id\":\"110108\"},{\"location\":\"116.294214,39.99685\",\"name\":\"西苑\",\"id\":\"110108\"}],\"building\":{\"name\":\"北京大学\",\"type\":\"科教文化服务;学校;高等院校\"},\"neighborhood\":{\"name\":\"北京大学\",\"type\":\"科教文化服务;学校;高等院校\"},\"citycode\":\"010\"},\"formatted_address\":\"北京市海淀区燕园街道北京大学\"},\"info\":\"OK\",\"infocode\":\"10000\"}"

    val jsonParse: JSONObject = JSON.parseObject(jsonStr)

    //判断状态是否成功

    val status: Int = jsonParse.getIntValue("status")

    if(status==0) {
      return ""
    }

    val regeocodeJson: JSONObject = jsonParse.getJSONObject("regeocode")

    if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

    val addressComponentJson: JSONObject = regeocodeJson.getJSONObject("addressComponent")

    if(addressComponentJson == null || addressComponentJson.keySet().isEmpty) return ""


    val businessAreasArrayJson = addressComponentJson.getJSONArray("businessAreas")

    if(businessAreasArrayJson == null || businessAreasArrayJson.isEmpty) return null

    val buffer=ListBuffer[String]()

    for(ele<-businessAreasArrayJson.toArray){

      if(ele.isInstanceOf[JSONObject]){

        val json: JSONObject = ele.asInstanceOf[JSONObject]

        buffer.append(json.getString("name"))
      }
    }

    buffer.mkString(",")


  }
}
