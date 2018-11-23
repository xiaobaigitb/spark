/*
  * @Title: SparkStreamingDemo
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/19:10:25
  */
/**
  * 实时处理数据
  */
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
object SparkStreamingDemo {
  def main(args: Array[String]): Unit = {
    //streaming至少两个线程
    val conf=new SparkConf().setMaster("local[*]").setAppName("streaming")
    //时间用于分割数据:Seconds(5)---5秒
    val ssc=new StreamingContext(conf,Seconds(1))

    //模拟从tcp端口读取数据
    val ds=ssc.socketTextStream("localhost",999)
    ds.map(word=>(word,1))
      .reduceByKey(_+_)
      .print()


    //启动streaming context，防止没有数据关闭
    //如果没有接受导数据，也不会立刻关闭，会尝试一段时间强制关闭
    ssc.start()
    ssc.awaitTermination()
  }
}
