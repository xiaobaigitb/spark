import org.apache.spark.{SparkConf, SparkContext}

/*
  * @Title: SparkYarnClient
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/15:15:11
  */
object SparkYarnClient {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf()
    /*.setMaster("local[*]")
    .setAppName("client")*/

    val sc=new SparkContext(sparkConf)

    val rdd=sc.textFile("hdfs://192.168.228.5:9000/sparkint/01.txt")
      //.foreach(println)
    rdd.flatMap(line=>line.split(" "))
      .map(x=>(x,1))
      .reduceByKey(_+_)
      .saveAsTextFile("hdfs://192.168.228.5:9000/sparkwcout1/")

    sc.textFile("hdfs://192.168.228.5:9000/sparkwcout1/part-00000").foreach(println)
  }
}
