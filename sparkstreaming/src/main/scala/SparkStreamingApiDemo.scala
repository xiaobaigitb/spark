import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
  * @Title: SparkStreamingApiDemo
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/19:11:12
  */
/**
  * streaming api
  */
object SparkStreamingApiDemo {
  def main(args: Array[String]): Unit = {
    //streaming至少两个线程
    val conf=new SparkConf().setMaster("local[*]").setAppName("streaming")
    //时间用于分割数据:Seconds(5)---5秒
    val ssc=new StreamingContext(conf,Seconds(5))

    //模拟从tcp端口读取数据
    val ds=ssc.socketTextStream("localhost",999)

    //foreachRDD:非常重要
    ds.foreachRDD(
      rdd=>rdd.map(word=>(word,1))
        .reduceByKey(_+_).collect().foreach(println)
    )

    //ds:DStream
    ds.transform(rdd=>rdd.map(word=>(word,1)).reduceByKey(_+_)).print()

    /**
      * todo
      */
    //持久化（保存的计算结果）（数据反复使用，避免多次计算），RDD优化--以空间换时间
    //这个--默认先内存（内存不够）后磁盘
    ds.persist()
    //指定将数据持久化到哪里（内存，还是磁盘）存储级别--不常用
    ds.persist(StorageLevel.MEMORY_AND_DISK)

    //启动streaming context，防止没有数据关闭
    ssc.start()
    ssc.awaitTermination()


//    ds.map(word=>(word,1))
//      .reduceByKey(_+_)
//      .print()

  }
}
