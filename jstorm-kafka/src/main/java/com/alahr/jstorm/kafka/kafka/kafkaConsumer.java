package com.alahr.jstorm.kafka.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class kafkaConsumer extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(kafkaConsumer.class);

    private String topic;

    public kafkaConsumer(String topic){
        super();
        this.topic = topic;
    }


    @Override
    public void run() {
        ConsumerConnector consumer = createConsumer();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
        Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
        ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();
        while(iterator.hasNext()){
            String message = new String(iterator.next().message());
            logger.info("data from kafka: " + message);
        }
    }

    public void run2() {
        ConsumerConnector consumer = createConsumer();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 5); // 一次从主题中获取五个数据
        Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = messageStreams.get(topic);
        for(KafkaStream<byte[], byte[]> stream : streams){
            ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();
            while(iterator.hasNext()){
                String message = new String(iterator.next().message());
                logger.info("接收到: " + message);
            }
        }
    }

    private ConsumerConnector createConsumer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "192.168.74.4:2181,192.168.74.5:2181,192.168.74.6:2181");//声明zk
        properties.put("group.id", "eyi_group");// 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }


    public static void main(String[] args) {
        new kafkaConsumer("eyi_topic").start();// 使用kafka集群中创建好的主题 test

    }
}
