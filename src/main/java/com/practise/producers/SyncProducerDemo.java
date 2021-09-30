package com.practise.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SyncProducerDemo {

    public static void main(String ...args){
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.RETRIES_CONFIG, 4);
        props.put(ProducerConfig.ACKS_CONFIG,1);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        try{
        for (int i = 1; i <= AppConfigs.numEvents; i++) {

                RecordMetadata metaData = producer.send(new ProducerRecord<>(AppConfigs.topicName, i, "Simple Message-" + i)).get();

                System.out.println("Message is sent to partition no: " + metaData.partition() + " and offset : " + metaData.offset());
                System.out.println("SynchronousProducer completed with success");


        }
        }catch(Exception e){
            e.printStackTrace();
        }
        finally{
            producer.close();
        }



    }
}
