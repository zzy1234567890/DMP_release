package com.utils

import java.util.Properties

import com.common.RedisConstant
import com.util.JedisUtil
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月24日
  *
  * @author 张振宇
  * @version ：1.0
  */
object JedisConnectionPool {

  val config=new JedisPoolConfig()

  config.setMaxTotal(20)

  config.setMaxIdle(10)

  val pool=new JedisPool(config,"NODE03",6379,10000)


  def getConnection():Jedis={
    pool.getResource
  }

}
