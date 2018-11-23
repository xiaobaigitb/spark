import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
  * @Title: SparkStreamingSqlDemo
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/19:11:32
  */
/**
  * spark Streaming sql
  */
object SparkStreamingSqlDemo {
  case class Word(word:String)
  def main(args: Array[String]): Unit = {
    //streaming至少两个线程
    val conf=new SparkConf().setMaster("local[*]").setAppName("streaming")
    //时间用于分割数据:Seconds(5)---5秒
    val ssc=new StreamingContext(conf,Seconds(5))

    //模拟从tcp端口读取数据
    val ds=ssc.socketTextStream("localhost",999)

    //创建sparksession对象
    val sparkSession=SparkSession.builder().master("local[*]")
      .appName("streamingsql").getOrCreate()

    import sparkSession.implicits._
    ds.foreachRDD(
      rdd=>{
        val df=rdd.map(word=>Word(word)).toDF()
        df.createOrReplaceTempView("tmp")
        sparkSession.sql("select word,count(word) wordcount from tmp group by word ")
          //.show()
          .write.format("parquet").save("d:\\test\\sparkstreamingsql"+"\\"+UUID.randomUUID().toString)
      }
    )


    //启动streaming context，防止没有数据关闭
    //如果没有接受导数据，也不会立刻关闭，会尝试一段时间强制关闭
    ssc.start()
    ssc.awaitTermination()
  }
}
