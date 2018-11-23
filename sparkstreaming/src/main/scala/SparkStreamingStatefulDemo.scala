/*
  * @Title: SparkStreamingDemo
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/19:10:25
  */
/**
  * 实时处理数据   有状态计算 需要还原点
  */

import org.apache.spark._
import org.apache.spark.streaming._ // not necessary since Spark 1.3
object SparkStreamingStatefulDemo {

  def functionToCreateContext(): StreamingContext = {
    //streaming至少两个线程
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    //时间用于分割数据:Seconds(5)---5秒
    val ssc = new StreamingContext(conf, Seconds(5))
    //模拟从tcp端口读取数据
    //设置还原点目录
    ssc.checkpoint("D:\\test\\checkpoint")
    val ds = ssc.socketTextStream("localhost", 999)

    //current:rdd的1放在里面   Option:可以没数据（第一次的时候没有数据）
    def update(current: Seq[Int], old: Option[Int]) = {
      val newValue = current.sum
      //第一次key出现时，没有状态，需要初始化
      val oldValue = old.getOrElse(0)

      //Some：Option下面的子类---hash--->map
      Some(newValue + oldValue)
    }

    ds.map(word => (word, 1))
      //update _:将方法转成函数 方法名 _
      .updateStateByKey(update _)
      .print()
    ssc
  }

  def main(args: Array[String]): Unit = {
    //    //streaming至少两个线程
    //    val conf=new SparkConf().setMaster("local[*]").setAppName("streaming")
    //    //时间用于分割数据:Seconds(5)---5秒
    //    val ssc=new StreamingContext(conf,Seconds(5))
    val ssc = StreamingContext.getOrCreate("D:\\test\\checkpoint", functionToCreateContext _)



    //启动streaming context，防止没有数据关闭
    //如果没有接受导数据，也不会立刻关闭，会尝试一段时间强制关闭
    ssc.start()
    ssc.awaitTermination()
  }
}
