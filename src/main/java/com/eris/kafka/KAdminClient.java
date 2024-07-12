package com.eris.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KAdminClient {

    private static final String BROKER_LIST = "localhost:9092";
    private static final String TOPIC = "TOPIC-B";


    /**
     * 创建topic
     *
     * @param adminClient
     */
    public static void createTopic(AdminClient adminClient) {
        NewTopic newTopic = new NewTopic(TOPIC, 4, (short) 1);

        CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));

        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    /*
     * 查看topic配置
     */
    public static void describeTopic(AdminClient adminClient) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC);

        DescribeConfigsResult result = adminClient.describeConfigs(Collections.singleton(configResource));

        try {
            Config config = result.all().get().get(configResource);
            System.out.println("kafka configs : " + JSON.toJSONString(config));
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }


    /**
     * 添加分区
     *
     * @param adminClient
     */
    public static void addPartition(AdminClient adminClient) {
        NewPartitions newPartitions = NewPartitions.increaseTo(5);
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        newPartitionsMap.put(TOPIC, newPartitions);

        try {
            CreatePartitionsResult result = adminClient.createPartitions(newPartitionsMap);
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        AdminClient adminClient = AdminClient.create(properties);

        // 1，创建topic
        createTopic(adminClient);

        // 2，添加分区
        addPartition(adminClient);

        // 3，查看所有topic
        describeTopic(adminClient);



        adminClient.close();

    }
}
