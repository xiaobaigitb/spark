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

/*
  优化了数据库连接池
 */
object OptimizedTrafficRestriction {

  def getfromOffsets(group: String) = {
    //Class.forName("com.mysql.jdbc.Driver")
    //val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    val conn =MysqlConnectionPool.getConnection()
    val select = conn.prepareStatement("select * from kafka_consumer where consumer_group=?")
    select.setString(1, group)
    val resultSet = select.executeQuery
    val partitionOffset: util.Map[TopicPartition, Long] = new util.HashMap[TopicPartition, Long]

    while (resultSet.next()) { //数据库有数据 说明不是第一次消费  first 置为false
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
    //获得最后一位数字
    var lastNum = 0;
    for (char <- carNum.toCharArray) {
      if (char >= 48 && char <= 57) {
        lastNum = char.toString.toInt
      }
    }
    println(lastNum)
    //获得今天是周几
    val week = getWeekOfDate(new Date())

    week match {
      case "1" => if (lastNum == 6 || lastNum == 1) true else false
      case "2" => if (lastNum == 7 || lastNum == 2) true else false
      case "3" => if (lastNum == 8 || lastNum == 3) true else false
      case "4" => if (lastNum == 9 || lastNum == 4) true else false
      case "5" => if (lastNum == 5 || lastNum == 0) true else false
      case _ => false
    }


  }

  /*def main(args: Array[String]): Unit = {
   println( judgeLegal("yuA89888"))
   println( judgeLegal("yuA89887"))
   println( judgeLegal("yuA89886"))
   println( judgeLegal("yuA89885"))
   println( judgeLegal("yuA89884"))
   println( judgeLegal("yuA89882"))
  }
*/
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(5))
    val group = "ssc2"
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
    val topics = Array("traffic")
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
        val conn = MysqlConnectionPool.getConnection()
        conn.setAutoCommit(false)
        val update = conn.prepareStatement("insert into kafka_consumer  (topic,`partition`,`offset`,consumer_group) values(?,?,?,?)  ON DUPLICATE KEY UPDATE `offset`=?")
        try {
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
          val sql = "insert into traffic_restriction (car_num) values (?)"
          val ps = conn.prepareStatement(sql)
          //业务逻辑  kv=>null  yua88888
          iter.map(record => (record.key, record.value)).filter(kv => judgeLegal(kv._2))
            .foreach(kv => {
              ps.setString(1, kv._2)
              println(kv._2 + ",limited")
              ps.addBatch()
            });

          ps.executeBatch()
          //保存偏移量
          update.setString(1, o.topic)
          update.setInt(2, o.partition)
          update.setLong(3, o.untilOffset)
          update.setString(4, group)
          update.setLong(5, o.untilOffset)
          update.executeUpdate
          conn.commit()
        } catch {
          case e: Exception => conn.rollback()
        } finally {
          MysqlConnectionPool.close(conn)
        }
      }

    })

    ssc.start()
    ssc.awaitTermination()
  }
}
