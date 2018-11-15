import org.apache.spark.{SparkConf, SparkContext}

/*
  * @Title: HdfsSource
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/15:11:35
  */
/**
  * 操作hdfs上的文件
  */
object HdfsSource {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("core")

    val sc=new SparkContext(sparkConf)

    val rdd=sc.textFile("hdfs://192.168.228.13:9000/wc/")

    rdd.flatMap(line=>line.split(" "))
      .map(x=>(x,1))
      .reduceByKey(_+_)
      .saveAsTextFile("hdfs://192.168.228.13:9000/sparkwcout/")
      //.collect().foreach(println)


  }
}
