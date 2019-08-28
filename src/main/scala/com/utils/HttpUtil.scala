package com.utils

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils


/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月24日
  *
  * @author 张振宇
  * @version ：1.0
  */
object HttpUtil {

  //Http请求协议
  def get(url:String):String={

    val client: CloseableHttpClient = HttpClients.createDefault()

    val get=new HttpGet(url)


    val response: CloseableHttpResponse = client.execute(get)

    EntityUtils.toString(response.getEntity,"UTF-8")

  }
}
