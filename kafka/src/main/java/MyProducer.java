import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

/**
 * @author Mr.lu
 * @Title: MyProducer
 * @ProjectName spark-scala
 * @Description: TODO
 * @date 2018/11/19:16:40
 */

/**
 * 生产者
 */
public class MyProducer extends Thread {
    private String topic;
    private KafkaProducer<String,String> producer;

    public MyProducer(String topic) {
        this.topic = topic;

        Properties properties = new Properties();
        properties.put("bootstrap.servers",KafkaProperties.broker);
        properties.put("acks","all");
        //todo 非常重要
        //幂等（0.11以后支持）--一个事情多次重复同一操作，产生的效果是一样的
        //客户端在向服务端消费的时候，带着ID，服务端保存这个ID，如果客户端再次请求，发现这个ID，会告诉已经请求过了
        properties.put("enable.idempotence","true");

        properties.put("acks","all");
        properties.put("transactional.id","abc");
        properties.put("batch.size","16384");
        properties.put("linger.ms","10");
        properties.put("buffer.memory","10");

        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(properties);
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (true){
            String message = "message" + messageNo;
            System.out.println("send = "+message);
            producer.send(new ProducerRecord<String, String>(topic,message));
            messageNo ++;

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args){
        new MyProducer(KafkaProperties.topic).start();
    }
}
