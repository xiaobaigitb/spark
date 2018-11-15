/*
  * @Title: WordCount
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/14:17:28
  */
package com.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

    def main(args: Array[String]): Unit = {
      //第一步：创建sparkConf
      val conf = new SparkConf()
        //设置spark的运行模式：本地模式（用来测试），standalone(spark集群模式)，yarn，Mesos,k8s
        //通常情况下：用本地模式测试（开发环境），用yarn部署（生产环境）
        //从spark2.3版本起支持k8s模式，未来spark程序大多会运行在k8s集群上
        //本地模式：local[x];x=1,代表启动一个线程，
        // X=2代表启动两个线程，x=* 代表启动跟所在机器cpu核数相等的线程数
        //注意：不能使用local[1],原因：spark本地模式是通过多线程来模拟分布式运行
        .setMaster("local[*]").setAppName("wordcount")
      //第二步：创建sparkContext
      val sc = new SparkContext(conf)

      //第三步：通过sparkcontext读取数据，生成rdd
      //4:尽可能与CPU核数对应
      val rdd=sc.textFile("D:\\test\\rootDir\\01.txt",4)
      //path:代表文件存储路径 minPartitions:最小分区数
      //分区是spark进行并行计算的基础
      println(rdd)
      //第四步：把rdd作为一个集合，进行各种运算

      rdd.flatMap(line=>line.split(" ")).map(x=>(x,1))
        .reduceByKey((a,b)=>a+b)//直接通过这个来实现WordCount
        .groupBy(x=>x)
        .map(kv=>(kv._1,kv._2.size))
        .saveAsTextFile("D:\\test\\rootDir\\wc")
        //.collect().foreach(println)

    }
}
