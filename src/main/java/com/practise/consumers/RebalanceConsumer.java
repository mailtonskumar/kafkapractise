package com.practise.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class RebalanceConsumer {

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String grp_id = "grp3";
        String topic = "topic2";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, grp_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        //creating consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        RebalanceListener rebalanceListner = new RebalanceListener(consumer);

        consumer.subscribe(Arrays.asList(topic),rebalanceListner);
        try{
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records){
                    System.out.println("Topic:"+ record.topic() +" Partition:" + record.partition() + " Offset:" + record.offset() + " Value:"+ record.value());
                    // Do some processing and save it to Database
                    rebalanceListner.addOffset(record.topic(), record.partition(),record.offset());
                }
                consumer.commitSync(rebalanceListner.getCurrentOffsets());
            }
        }catch(Exception ex){
            System.out.println("Exception.");
            ex.printStackTrace();
        }
        finally{
            consumer.close();
        }



    }



}
