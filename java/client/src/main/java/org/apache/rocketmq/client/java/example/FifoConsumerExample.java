package org.apache.rocketmq.client.java.example;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Step1 topic设置为FIFO
 * ./bin/mqadmin updateTopic -c DefaultCluster -t MyOrderTopic -o true -n 127.0.0.1:9876 -a +message.type=FIFO
 * Step2 消费组设置为FIFO
 *
 */
public class FifoConsumerExample {
    private static final Logger log = LoggerFactory.getLogger(FifoConsumerExample.class);

    public static void main(String[] args) throws ClientException, IOException, InterruptedException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        String endpoints = "127.0.0.1:8081;127.0.0.1:8091";
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(endpoints)
            .build();
        Map<String, FilterExpression> subscription = new HashMap<>();
//        subscription.put("MyTopic", FilterExpression.SUB_ALL); // 消费组被设置为顺序，普通消息一样按照顺序处理
        subscription.put("MyOrderTopic", FilterExpression.SUB_ALL); // 顺序消息
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            // Set the consumer group name.
            .setConsumerGroup("fifoConsumer1")
            // Set the subscription for the consumer.
            .setSubscriptionExpressions(subscription)
            .setMessageListener(messageView -> {
                // Handle the received message and return consume result.
                log.info("Consume message={}", messageView);
//                try {
//                    Thread.sleep(60 * 1000 * 2);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//                log.info("Consume SUCCESS message={}", messageView);
                return ConsumeResult.SUCCESS;
            })
            .build();
        // Block the main thread, no need for production environment.
        Thread.sleep(Long.MAX_VALUE);
        // Close the push consumer when you don't need it anymore.
        pushConsumer.close();
    }
}
