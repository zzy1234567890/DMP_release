package com.utils

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月20日
  *
  * @author 张振宇
  * @version ：1.0
  */
object Utils2Type {

  def toInt(str:String):Int={
    try {
      str.toInt
    } catch {
      case e:Exception =>0
    }
  }

  def toDouble(str:String):Double={
    try {
      str.toDouble
    } catch {
      case e:Exception =>0.0
    }
  }
}
