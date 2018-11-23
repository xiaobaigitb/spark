/**
 * @author Mr.lu
 * @Title: KafkaConsumerTransactionDemo
 * @ProjectName spark-scala
 * @Description: TODO
 * @date 2018/11/20:14:05
 */
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.sql.*;
import java.util.*;
/*
注意:数据库建表  是 不能四个都建立唯一索引  需要 topic  partition consumergroup 三个建立唯一索引。
offset不能建立唯一索引
 */
public class KafkaConsumerTransactionDemo {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        String topic = "lol";
        int partition = 0;
        long offset = 0;
        String consumerGroup = "f";
        //定义标志 是否是第一次消费
        boolean first=true;

        Properties properties = new Properties();

        properties.put("bootstrap.servers", "192.168.228.13:9092");

        properties.put("group.id", consumerGroup);

        properties.put("enable.auto.commit", "false");

        properties.put("auto.commit.interval.ms", "1000");

        properties.put("auto.offset.reset", "earliest");//

        properties.put("session.timeout.ms", "30000");

        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer(properties);


        //连接数据库 获取偏移量
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123");
        //自动提交--false
        connection.setAutoCommit(false);
        PreparedStatement select = connection.prepareStatement("select * from kafka_consumer where consumer_group=?");
        select.setString(1, consumerGroup);
        ResultSet resultSet = select.executeQuery();
        //定义map  保存分区与偏移量信息
        Map<TopicPartition, Long> partitionOffset = new HashMap<TopicPartition, Long>();

        while (resultSet.next()) {
            //数据库有数据 说明不是第一次消费  first 置为false
            first=false;
            topic = resultSet.getString(1);
            partition = resultSet.getInt(2);
            offset = resultSet.getLong(3);
            consumerGroup = resultSet.getString(4);
            TopicPartition tp = new TopicPartition(topic, partition);
            partitionOffset.put(tp, offset);
            //kafkaConsumer.seek(tp, offset);
        }
        //如果是第一次  则 直接订阅topic
        if (first) {

            kafkaConsumer.subscribe(Arrays.asList(topic));
        } else {

            //不是第一次  从上次消费的偏移量开始

            Set<TopicPartition> topicPartitions = partitionOffset.keySet();
            //为消费者分配分区
            kafkaConsumer.assign(topicPartitions);
            //遍历每个分区  指定每个分区开始的地方
            for (TopicPartition tp :
                    topicPartitions) {
                //下次消费 应该从上次offset+1的地方开始
                kafkaConsumer.seek(tp, partitionOffset.get(tp)+1);
            }

        }

        while (true) {
            try {
                Thread.sleep(200);
                //"insert into kafka_consumer  (topic,`partition`,`offset`,consumer_group) values(?,?,?,?)

                // ON DUPLICATE KEY UPDATE `offset`=?"
                PreparedStatement update = connection.prepareStatement("insert into kafka_consumer  (topic,`partition`,`offset`,consumer_group) values(?,?,?,?)  ON DUPLICATE KEY UPDATE `offset`=?");
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                    System.out.printf("partition = %d, key = %s", record.partition(), record.key());
                    System.out.printf("time = %d, topic = %s", record.timestamp(), record.topic());
                    System.out.println("do something");
                    update.setString(1, topic);
                    update.setInt(2, record.partition());
                    update.setLong(3, record.offset());
                    update.setString(4, consumerGroup);
                    update.setLong(5, record.offset());
                    update.executeUpdate();
                    connection.commit();
                }

            } catch (Exception e) {
                System.out.println(e);
                connection.rollback();
            } finally {

            }
            // kafkaConsumer.commitSync();
        }

    }
}