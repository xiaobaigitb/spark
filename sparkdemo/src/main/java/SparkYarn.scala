import org.apache.commons.io.IOUtils
import org.apache.spark.{SparkConf, SparkContext}

/*
  * @Title: SparkYarn
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/15:11:49
  */
object SparkYarn {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("core")

    val sc=new SparkContext(sparkConf)

    val rdd=sc.textFile("hdfs://192.168.228.13:9000/wc/")
    rdd.flatMap(line=>line.split(" "))
      .map(x=>(x,1))
      //.reduceByKey(_+_)
      .reduceByKey((a,b)=>a+b)
      .saveAsTextFile("hdfs://192.168.228.13:9000/sparkwcout/")
  }
}
