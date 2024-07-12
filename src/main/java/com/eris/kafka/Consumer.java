package com.eris.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.eris.kafka.util.LogUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class Consumer {

    private static final String BROKER_LIST = "localhost:9092";
    private static final String TOPIC_A = "TOPIC-A";
    private static final String TOPIC_B = "TOPIC-B";
    private static final String TOPIC_C = "TOPIC-C";
    private static final String GROUP_ID_A = "GROUP-A";
    private static final String GROUP_ID_B = "GROUP-B";
    private static final String GROUP_ID_C = "GROUP-C";
    private static final AtomicBoolean IS_RUNNING = new AtomicBoolean(true);

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        List<Logger> loggerList = loggerContext.getLoggerList();
        loggerList.forEach(logger -> {
            logger.setLevel(Level.INFO);
        });

    }

    public static Properties initConfig() {
        Properties properties = new Properties();
        // 以下3个必须
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 客户端ID
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "eris-kafka-consumer");

        // 消费组ID
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_C);

        // 自动提交，默认为true
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(TOPIC_B));

        try {
            while (IS_RUNNING.get()) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                LogUtil.printWithPrefix("拉取到消息:" + records.count());

                for (ConsumerRecord<String, String> r : records) {
                    LogUtil.printWithPrefix("topic:" + r.topic() + ", patition:" + r.partition() + ", offset:" + r.offset());
                    LogUtil.printWithPrefix("key:" + r.key() + ", value:" + r.value());
                }

//                // 根据分区消费
//                for (TopicPartition tp : records.partitions()) {
//                    for (ConsumerRecord<String, String> record : records.records(tp)) {
//                        System.out.println(record.partition() + ":" + record.value());
//                    }
//                }
//
//                // 根据topic消费
//                for (String topic : Arrays.asList(TOPIC)) {
//                    for (ConsumerRecord<String, String> record : records.records(topic)) {
//                        System.out.println(record.topic() + ":" + record.value());
//                    }
//                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }

    public void test(){

    }
}
