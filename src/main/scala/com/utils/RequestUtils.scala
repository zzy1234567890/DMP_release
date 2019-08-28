package com.utils

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月21日
  *
  * @author 张振宇
  * @version ：1.0
  */
object RequestUtils {

  def request(requestmode:Int,processnode:Int):List[Double]={

//    val list=List[Double]()
//
//    if (requestmode==1 && processnode>=1) list :+ 1 else list:+0
//
//    if (requestmode==1 && processnode>=2) list :+ 1 else list:+0
//
//    if (requestmode==1 && processnode==3) list :+ 1 else list :+0
//
//   list

    if (requestmode==1 && processnode>=1){
      List[Double](1,0,0)
    } else if (requestmode==1 && processnode >=2){
      List[Double](1,1,0)
    } else if (requestmode==1 && processnode ==3){
      List[Double](1,1,1)
    } else {
      List[Double](0,0,0)
    }



  }

  def click(requestmode:Int,iseffective:Int):List[Double]={

//    val list1=List[Double]()
//
//    if(requestmode==2 && iseffective==1) list1:+1 else list1:+0
//
//    if(requestmode==3 && iseffective==1) list1:+1 else list1:+0
//
//    list1

    if(requestmode==2 && iseffective==1) {
      List[Double](1,0)
    } else if (requestmode==3 && iseffective==1){
      List[Double](0,1)
    }else {
      List[Double](0,0)
    }

  }

  def cost(iseffective:Int,isbilling:Int,iswin:Int,adorderid:Int,winprice:Double,adpayment:Double):List[Double]={

//    val list2=List[Double]()
//
//    if(iseffective==1 && isbilling==1) list2:+1 else list2:+0
//
//    if (iseffective==1 && isbilling==1 && iswin==1 && adorderid !=0) list2:+1 else list2:+0
//
//    if (iseffective==1 && isbilling==1 && iswin == 1  ) list2:+winprice else list2:+0
//
//    if (iseffective==1 && isbilling==1 && iswin == 1  ) list2:+ adpayment else list2:+0
//
//    list2


    if (iseffective==1 && isbilling==1) {
      if (iseffective==1 && isbilling==1 && iswin==1 && adorderid !=0){
        List[Double](1,1,winprice/1000.0,adpayment/1000.0)
      }else{
        List[Double](1,0,0,0)
      }

    }else {
      List[Double](0,0,0,0)
    }

  }
}
