import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author Mr.lu
 * @Title: MyConsumer
 * @ProjectName spark-scala
 * @Description: TODO
 * @date 2018/11/19:16:42
 */

/**
 * 消费者
 */
public class MyConsumer extends Thread {
    private String topic;
    KafkaConsumer<String, String> consumer;

    public MyConsumer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KafkaProperties.broker);
        properties.setProperty("group.id","test");
        properties.setProperty("enable.auto.commit","false");
        properties.setProperty("auto.offset.reset","earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(KafkaProperties.topic));
    }

    @Override
    public void run() {
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> recode: records) {
                System.out.println("recodeOffset = " + recode.offset() + "recodeValue = " + recode.value());
            }
            //手动同步提交
            consumer.commitSync();
        }
    }
    public static void main(String[] args){
        new MyConsumer().start();
    }
}
