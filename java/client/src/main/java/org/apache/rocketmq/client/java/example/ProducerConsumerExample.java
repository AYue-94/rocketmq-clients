package org.apache.rocketmq.client.java.example;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerConsumerExample {

    private static final Logger log = LoggerFactory.getLogger(ProducerConsumerExample.class);

    private static final String access_key = "RocketMQ";

    private static final String secret_key = "12345678";

//    private static final String proxy_endpoints = "127.0.0.1:8081;127.0.0.1:8091";

    private static final String proxy_endpoints = "rmq-proxy.local:443";

    private static final String topic = "MyTopic";

    private static final String tag = "A";

    public static void main(String[] args) throws InterruptedException, ClientException, IOException {
        Producer producer = createProducer();
        PushConsumer consumer = createConsumer();
        byte[] body = "Hello".getBytes(StandardCharsets.UTF_8);
        final Message message = ClientServiceProvider.loadService().newMessageBuilder()
            .setTopic(topic)
            .setTag(tag)
            .setKeys("123")
            .setBody(body)
            .build();
        while (true) {
            try {
                Thread.sleep(1000);
                final SendReceipt sendReceipt = producer.send(message);
                log.info("Send message successfully, messageId={}", sendReceipt.getMessageId());
            } catch (Throwable t) {
                log.error("Failed to send message", t);
                break;
            }
        }
        producer.close();
        consumer.close();
    }

    private static Producer createProducer() throws ClientException {
        SessionCredentialsProvider sessionCredentialsProvider =
            new StaticSessionCredentialsProvider(access_key, secret_key);

        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(proxy_endpoints) // proxy端点，分号分割
            .setRequestTimeout(Duration.ofDays(1)) // 请求超时时间
            .setCredentialProvider(sessionCredentialsProvider) // ak sk
//            .enableSsl(false)
            .build();

        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        return provider.newProducerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setTopics(topic)
            .build();
    }

    private static PushConsumer createConsumer() throws ClientException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        SessionCredentialsProvider sessionCredentialsProvider =
            new StaticSessionCredentialsProvider(access_key, secret_key);
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(proxy_endpoints)
            .setCredentialProvider(sessionCredentialsProvider)
            .build();
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        String consumerGroup = "groupABC";
        return provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            .setConsumerGroup(consumerGroup)
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            .setMessageListener(messageView -> {
                log.info("Consume message={}", messageView);
                return ConsumeResult.SUCCESS;
            })
            .build();
    }
}
