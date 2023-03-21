package com.logiclabent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private static final Logger log= LoggerFactory.getLogger(Consumer.class);

    public static void main(String [] args)
    {
       log.info("Consumer started");
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"lotr_consumer_group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        try
        {
            String topic = "lotr_characters";
            consumer.subscribe(Arrays.asList(topic));

            while (true){
                ConsumerRecords<String,String> messages= consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String,String> message: messages){
                    log.info("key ["+message.key()+"] value["+message.value()+"]");
                    log.info("partition ["+message.partition()+"]");
                }
            }

        }catch (Exception err)
        {
           log.error("Error ",err);
        }finally {
           consumer.close();
           log.info("The consumer is now closed");
        }

    }
}
