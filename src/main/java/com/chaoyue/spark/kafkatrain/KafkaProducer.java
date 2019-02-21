package com.chaoyue.spark.kafkatrain;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Kafka生产者
 */
public class KafkaProducer extends Thread{

    private String topic;

    //发送信息需要这类，就是kafka的producerAPI，借助它来发送信息
    private Producer<Integer, String> producer;

    public KafkaProducer(String topic){
        this.topic = topic;

        //java Properties类 读取配置文件的类 为producer提供必要的配置
        Properties properties = new Properties();

        properties.put("metadata.broker.list", KafkaProperties.BROKER_LIST);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks","1");//ack级别，1表示leader收到就好

        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }

    @Override
    public void run() {

        int messageNo = 1;

        while (true){
            String message = "there could be " + messageNo + " people in this room";
            producer.send(new KeyedMessage<Integer, String>(topic, message));
            System.out.println("Lady gaga said: " + message);

            messageNo ++ ;

            try {
                Thread.sleep(2000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }

    }
}
