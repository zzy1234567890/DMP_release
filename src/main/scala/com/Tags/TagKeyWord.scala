package com.Tags

import org.apache.spark.broadcast.Broadcast
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
object TagKeyWord extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list=List[(String,Int)]()

    val row: Row = args(0).asInstanceOf[Row]

    val stopWords: Broadcast[collection.Map[String, Int]] = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]

    val keyWords: String = row.getAs[String]("keywords")

//    val keyWordArr: Array[String] = keyWords.split("\\|").filter(x=>x.length>=3 && x.length<=8 && !stopWords.value.contains(x))
//
//    for(ele<-keyWordArr){
//      list :+ ("K"+ele,1)
//    }

        keyWords.split("\\|").filter(x => x.length >= 3 && x.length <= 8 && !stopWords.value.contains(x))
      .foreach(word => list :+= ("K" + word, 1))


    list
  }
}
