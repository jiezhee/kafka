package com.eris.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class Consumer {

    private static final String BROKER_LIST = "localhost:9092";
    private static final String TOPIC = "TOPIC-A";
    private static final String GROUP_ID = "GROUP-A";
    private static final AtomicBoolean IS_RUNNING = new AtomicBoolean(true);


    public static Properties initConfig() {
        Properties properties = new Properties();
        // 以下3个必须
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 客户端ID
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "eris-kafka-consumer");

        // 消费组ID
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        // 自动提交，默认为true
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(TOPIC));

        try {
//            while (IS_RUNNING.get()) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> r : records) {
                    System.out.println("topic:" + r.topic() + ", patition:" + r.partition() + ", offset:" + r.offset());
                    System.out.println("key:" + r.key() + ", value:" + r.value());
                }
//            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }

}
