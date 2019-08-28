package com.Graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description:xxxx<br/>
  * Copyright (c), 2019, zhangzhenyu<br/>
  * This  programis protected by copyright laws.<br/>
  * Date: 2019年08月26日
  *
  * @author 张振宇
  * @version ：1.0
  */
object graphTest {
  def main(args: Array[String]): Unit = {


    val conf= new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")

    val sc = new SparkContext(conf)

    val vertexRDD: RDD[(Long, (String, String))] = sc.parallelize(Seq(
      (1L, ("1号", "上")),
      (2L, ("2号", "左")),
      (6L, ("6号", "下")),
      (9L, ("9号", "右")),
      (133L, ("133", "中心")),
      (138L, ("138", "中心")),
      (16L, ("16号", "左")),
      (44L, ("44号", "下")),
      (21L, ("21号", "右")),
      (5L, ("5号", "下")),
      (7L, ("7号", "上")),
      (158L, ("158号", "中心"))
    ))

    val edge: RDD[Edge[Int]] = sc.parallelize(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

    //构建图
    val graph: Graph[(String, String), Int] = Graph(vertexRDD,edge)

    //取出每个边上的最大顶点  vertices为vertex的复数
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    vertices.join(vertexRDD).map{

      case (userId,(conId,(x,y))) =>{

        (conId,List((x,y)))
      }
    }.reduceByKey(_++_).foreach(println)




  }
}
