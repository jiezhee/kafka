package com.eris.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    private static final String BROKER_LIST = "localhost:9092";
    private static final String TOPIC = "TOPIC-A";


    public static Properties initConfig() {
        Properties properties = new Properties();
        // 以下3个必须
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // 客户端ID
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "eris-kafka-producer");

        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 成功收到消息分区副本数，默认为1，即leader副本收到就返回成功。注意是字符串！
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        // 生产者客户端能发送的消息的最大值，单位为B，默认1M
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,  1048576);

        // Producer等待请求响应的最长时间，单位ms，默认值为30000
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,  30000);

        // 生产者发送ProducerBatch之前等待更多ProducerRecord加入的时间。默认为0，ProducerBatch被填满时发出
        properties.put(ProducerConfig.LINGER_MS_CONFIG,  0);
        return properties;
    }

    public static ProducerRecord<String, String> initMessage() {
        return new ProducerRecord<>(TOPIC, "hi eris");
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        ProducerRecord record = initMessage();

        try {
            kafkaProducer.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
