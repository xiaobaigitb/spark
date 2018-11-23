import java.sql.{DriverManager, PreparedStatement, ResultSet}
import java.util
import java.util.Date

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object TrafficRestriction {

  def getfromOffsets(group: String) = {
    Class.forName("com.mysql.jdbc.Driver")
    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123")

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
    import scala.collection.JavaConverters._
    partitionOffset.asScala
  }


  import java.util.Calendar

  def getWeekOfDate(dt: Date): String = {
    val weekDays = Array("7", "1", "2", "3", "4", "5", "6")
    val cal = Calendar.getInstance
    cal.setTime(dt)
    var w = cal.get(Calendar.DAY_OF_WEEK) - 1
    if (w < 0) w = 0
    weekDays(w)
  }

  def judgeLegal(carNum: String): Boolean = {
    //将车牌号变成字节数组
    val carNums = carNum.toCharArray
    //获取车牌号的最后一位数字
    var lastNum = 0
    for (char <- carNums) {
      if (char >= 48 && char <= 57) {
        lastNum = char.toString.toInt
      }
    }
    println(lastNum)
    //获取今天是周几
    val week = getWeekOfDate(new Date())

    //使用模式匹配
    //1-->1,6
    week match {
      case "1" => if (lastNum == 1 || lastNum == 6) true else false
      case "2" => if (lastNum == 2 || lastNum == 7) true else false
      case "3" => if (lastNum == 3 || lastNum == 8) true else false
      case "4" => if (lastNum == 4 || lastNum == 9) true else false
      case "5" => if (lastNum == 5 || lastNum == 0) true else false
      case _ => false
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(5))
    val group = "ssc"
    //获取保存在数据库的偏移量
    val fromOffsets = getfromOffsets(group)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.228.13:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("t1")

    //第一次
    var stream: DStream[ConsumerRecord[String, String]] = null
    if (fromOffsets.size == 0) {
      stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
    } else {
      stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
      )
    }
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
        val update = conn.prepareStatement("insert into kafka_consumer  (topic,`partition`,`offset`,consumer_group) values(?,?,?,?)  ON DUPLICATE KEY UPDATE `offset`=?")
        conn.setAutoCommit(false)
        try {
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
          //将结果插入数据库
          val sql = "insert into traffic_restriction (car_num) values (?)"
          val ps = conn.prepareStatement(sql)

          //业务逻辑处理
          iter.map(record => (record.key, record.value))
            .filter(kv => judgeLegal(kv._2))
            //进行保存造数据库
            .foreach(kv => {
            ps.setString(1, kv._2)
            println(kv._2 + ",limited")
            //添加缓冲--批量插入  提高性能
            ps.addBatch()
          })
          //提交缓存-批量插入
          ps.executeBatch()

          update.setString(1, o.topic)
          update.setInt(2, o.partition)
          update.setLong(3, o.untilOffset)
          update.setString(4, group)
          update.setLong(5, o.untilOffset)
          update.executeUpdate
          conn.commit()
        } catch {
          case e: Exception => conn.rollback()
        }
      }

    })

    ssc.start()
    ssc.awaitTermination()
  }
}
