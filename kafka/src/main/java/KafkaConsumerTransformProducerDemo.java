import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Mr.lu
 * @Title: KafkaConsumerTransformProduceDemo
 * @ProjectName spark-scala
 * @Description: TODO
 * @date 2018/11/20:15:14
 */
public class KafkaConsumerTransformProducerDemo {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = getConsumer();
        KafkaProducer<String, String> producer = getProducer();

        //开启事务
        producer.initTransactions();
        producer.beginTransaction();
        while (true) {
            try {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                Map<TopicPartition, OffsetAndMetadata> hashMap = new HashMap();
                for (ConsumerRecord record :
                        consumerRecords) {
                    System.out.println(record.value());
                    if (record.value().toString().equals("h")) {
                        producer.send(new ProducerRecord<String, String>("t2", (String) record.value()));
                        producer.flush();
                        hashMap.put(new TopicPartition("t1", record.partition()), new OffsetAndMetadata(record.offset()));

                    }
                }
                producer.sendOffsetsToTransaction(hashMap, "f");
                //提交事务
                producer.commitTransaction();
            } catch (Exception e) {
                //事务回滚
                producer.abortTransaction();
            }

        }
    }

    //生产者
    private static KafkaProducer<String, String> getProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaProperties.broker);
        properties.put("acks", "all");
        //todo 非常重要
        //幂等（0.11以后支持）--一个事情多次重复同一操作，产生的效果是一样的
        //客户端在向服务端消费的时候，带着ID，服务端保存这个ID，如果客户端再次请求，发现这个ID，会告诉已经请求过了
        properties.put("enable.idempotence", "true");

        properties.put("transactional.id", "abc");
        properties.put("batch.size", "16384");
        properties.put("linger.ms", "10");
        properties.put("buffer.memory", "10");

        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(properties);
    }

    //消费者
    private static KafkaConsumer<String, String> getConsumer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KafkaProperties.broker);
        properties.setProperty("group.id", "f");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(KafkaProperties.topic));
        return kafkaConsumer;
    }
}
