//package kafka.day01;
//
///**
// * @author Mr.lu
// * @Title: SparkStreamingFromkafka
// * @ProjectName spark-scala
// * @Description: TODO
// * @date 2018/11/19:17:31
// */
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.rdd.RDD;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaInputDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.ConsumerStrategies;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//import org.apache.spark.streaming.kafka.LocationStrategies;
//
//import scala.Tuple2;
//
//public class SparkStreamingFromkafka {
//
//    public static void main(String[] args) throws Exception {
//        // TODO Auto-generated method stub
//        SparkConf sparkConf  = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingFromkafka");
//        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf , Durations.seconds(1));
//
//        Map<String, Object> kafkaParams = new HashMap();
//        kafkaParams.put("bootstrap.servers", "192.168.246.134:9092");//多个可用ip可用","隔开
//        kafkaParams.put("key.deserializer", StringDeserializer.class);
//        kafkaParams.put("value.deserializer", StringDeserializer.class);
//        kafkaParams.put("group.id", "sparkStreaming");
//        Collection<String> topics = Arrays.asList("video");//配置topic，可以是数组
//
//        JavaInputDStream<ConsumerRecord<String, String>> javaInputDStream =KafkaUtils.createDirectStream(
//                streamingContext,
//                LocationStrategies.PreferConsistent(),
//                ConsumerStrategies.Subscribe(topics, kafkaParams));
//
//        JavaPairDStream<String, String> javaPairDStream = javaInputDStream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>(){
//            private static final long serialVersionUID = 1L;
//            //@Override
//            public Tuple2<String, String> call(ConsumerRecord<String, String> consumerRecord) throws Exception {
//                return new Tuple2(consumerRecord.key(), consumerRecord.value());
//            }
//        });
//        javaPairDStream.foreachRDD(new VoidFunction<JavaPairRDD<String,String>>() {
//            //@Override
//            public void call(JavaPairRDD<String, String> javaPairRDD) throws Exception {
//                // TODO Auto-generated method stub
//                javaPairRDD.foreach(new VoidFunction<Tuple2<String,String>>() {
//                    //@Override
//                    public void call(Tuple2<String, String> tuple2)
//                            throws Exception {
//                        // TODO Auto-generated method stub
//                        System.out.println(tuple2._2);
//                    }
//                });
//            }
//        });
//        streamingContext.start();
//        streamingContext.awaitTermination();
//    }
//
//}
