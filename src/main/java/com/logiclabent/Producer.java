package com.logiclabent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import org.apache.

public class Producer {
    private static final Logger log= LoggerFactory.getLogger(Producer.class);

    public static void main(String [] args)
    {
        log.info("This class will produce messages to Kafka");
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);


    }
}
