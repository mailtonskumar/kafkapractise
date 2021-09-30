package com.practise.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomPartitioner {

    public static void main(String ...args){

        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("speed.sensor.name", "TSS");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SensorPartitioner.class);
        Producer<String, String> producer = new KafkaProducer <>(props);

        for (int i=0 ; i<10 ; i++)
            producer.send(new ProducerRecord<>("sensor","SSP"+i,"500"+i));

        for (int i=0 ; i<10 ; i++)
            producer.send(new ProducerRecord<>("sensor","TSS","500"+i));

        producer.close();

        System.out.println("SimpleProducer Completed.");

    }
}
