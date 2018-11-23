package com.tuning

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object DataBias {
  //解决聚合情况下的数据倾斜问题
  //key前加随机数，聚合，再去掉随机前缀
  def testAcc(sc: SparkContext) = {


    //sc.parallelize(List("hello", "hello", "hello", "hello", "world"))
    sc.textFile("d:\\test\\ssc\\bias.txt",20)
    .map(word => (word, 1))

      //传统做法，可能会出现数据倾斜
      .reduceByKey(_+_)
      /*.map { case (key, value) => {
      val random = new Random();
      (random.nextInt(3) + "_" + key, value)
    }
    }
      .reduceByKey(_ + _)
      .map { case (k, v) => (k.substring(k.indexOf("_") + 1), v) }
      .reduceByKey(_ + _)*/
      .foreach(println)
    Thread.sleep(1000000)
  }

  def testJoin(sc: SparkContext): Unit = {
      val rddl=sc.parallelize(List((1,"hello"),(1,"hello"),(1,"hello"),(1,"hello"),(2,"world")))
      val rddr=sc.parallelize(List((1,"man"),(2,"woman")))
    //传统方式，可能会出现数据倾斜
    //rddl.join(rdd2).foreach(println)
    //左侧rdd加随机前缀（n以内），右侧rdd根据随机前缀扩容n倍
    val prefixRdd=rddl.map{case (k,v)=>{
      val random = new Random();
      (random.nextInt(3) + "_" + k, v)
    }}

    //右侧扩容
    val expandRdd=rddr.flatMap{
      case (k,v)=>{
        val num=List(0,1,2)
        num.map(i=>(i+"_"+k,v))
      }
    }
    //去掉前缀
  prefixRdd.join(expandRdd)
      .map{case (k,v)=>(k.split("_")(1),v)}
    .foreach(println)

  }


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]")
      .setAppName("data bias")
    val sc = new SparkContext(sparkConf)
    testAcc(sc)
    //testJoin(sc)
  }

}
