package com.utils

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月23日
  *
  * @author 张振宇
  * @version ：1.0
  */
trait Tag {

  //打标签的统一接口

  def makeTags(args:Any*):List[(String,Int)]


}
