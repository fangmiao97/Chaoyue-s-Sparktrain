package com.chaoyue.spark.kafka;

/**
 * Kafka producer API测试
 */
public class KafkaClinetApp {

    public static void main(String[] args) {

        new KafkaConsumer(KafkaProperties.TOPIC).start();
        new KafkaProducer(KafkaProperties.TOPIC).start();

    }
}
