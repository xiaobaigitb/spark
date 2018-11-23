
import java.util
import java.sql.DriverManager
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign

/*
  * @Title: SparkStreamingKafkaSourceDemo
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/20:16:25
  */
/**
  * sparkStreaming 分析kafka的数据
  */
object SparkStreamingKafkaSourceDemo {
  def main(args: Array[String]): Unit = {
    //statictest.noAutoSubmit
    //statictest.shouSubmit
    //statictest.autoSubmit
  }
}

object statictest{

  def autoSubmit: Unit ={

    def getfromOffsets(group:String) = {
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123")
      //通过消费者组，获取kafka的消费者对象
      val select = conn.prepareStatement("select * from kafka_consumer where consumer_group=?")
      select.setString(1, group)
      val resultSet = select.executeQuery
      val partitionOffset: util.Map[TopicPartition, Long] = new util.HashMap[TopicPartition, Long]

      while (resultSet.next()) {
        //数据库有数据 说明不是第一次消费  first 置为false
        val tp: TopicPartition = new TopicPartition(resultSet.getString(1), resultSet.getInt(2))
        partitionOffset.put(tp, resultSet.getLong(3))
      }
      resultSet.close()
      conn.close()
      //将Java集合，转化成scala集合
      import scala.collection.JavaConverters._
      partitionOffset.asScala
    }

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
      val ssc = new StreamingContext(conf, Seconds(5))
      val group = "ssc"
      //获取保存在数据库的偏移量
      val fromOffsets=getfromOffsets(group)

      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> group,
        "auto.offset.reset" -> "earliest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
      val topics = Array("t1")

      val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
      )


      stream //.map(record => (record.key, record.value))
        .foreachRDD(rdd => {
        //获取偏移量
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //rdd.foreach(record => println(record.key() + ":" + record.value))
        //业务逻辑处理完，保存偏移量到kafka
        //stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        rdd.foreachPartition { iter =>
          Class.forName("com.mysql.jdbc.Driver")
          val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123")
          //第一次插入，后面的更新
          val update = conn.prepareStatement("insert into kafka_consumer  (topic,`partition`,`offset`,consumer_group) values(?,?,?,?)  ON DUPLICATE KEY UPDATE `offset`=?")
          //关闭自动提交--因为事务
          conn.setAutoCommit(false)
          try {
            //获取消费者在当前分区的消费偏移量对象
            val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
            println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")

            //中间处理
            iter.foreach(record => println(record.key() + ":" + record.value))

            //保存到数据库
            update.setString(1, o.topic)
            update.setInt(2, o.partition)
            update.setLong(3, o.untilOffset)
            update.setString(4, group)
            update.setLong(5, o.untilOffset)
            update.executeUpdate
            //提交
            conn.commit()
          } catch {
            //异常，回滚
            case e: Exception => conn.rollback()
          }
        }
      })

      ssc.start()
      ssc.awaitTermination()
    }
  }

  //不提交偏移量
  def noAutoSubmit: Unit ={
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(5))
    import org.apache.kafka.common.serialization.StringDeserializer
    import org.apache.spark.streaming.kafka010._
    import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
    import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.228.13:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "ssc1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("t1")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => (record.key, record.value))
        .print()

    ssc.start()
    ssc.awaitTermination()
  }

  //手动提交将偏移量保存在kafka
  def shouSubmit: Unit ={
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(5))
    import org.apache.kafka.common.serialization.StringDeserializer
    import org.apache.spark.streaming.kafka010._
    import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
    import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.228.13:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "ssc",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("t1")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream//.map(record => (record.key, record.value))
      .foreachRDD(rdd=>{
      //获取偏移量
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreach(record=>println(record.key()+":"+record.value))
      //业务逻辑处理完，保存偏移量到kafka
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    })

    ssc.start()
    ssc.awaitTermination()
  }

}
