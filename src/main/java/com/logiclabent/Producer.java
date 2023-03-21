package com.logiclabent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
    private static final Logger log= LoggerFactory.getLogger(Producer.class);

    public static void main(String [] args)
    {
        log.info("This class will produce messages to Kafka");
    }
}
