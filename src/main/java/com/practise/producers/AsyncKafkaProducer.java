package com.practise.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Callable;

public class AsyncKafkaProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //logger.info("Start sending messages...");
        String key = "key-1";
        String value = "Value-1";

        ProducerRecord<String, String> record = new ProducerRecord<>(AppConfigs.topicName,key,value);
        producer.send(record, new MyProducerCallback());
        System.out.println("AsynchronousProducer call completed");
        producer.close();
    }

}

class MyProducerCallback implements Callback {


    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {

        if(e!= null){
            System.out.println("Async call failed" + e.getMessage());
        }
        System.out.println("Async call success");
    }
}
