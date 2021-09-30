package com.practise.consumers;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RebalanceListener implements ConsumerRebalanceListener {

    private KafkaConsumer consumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap();

    public RebalanceListener (KafkaConsumer con){
        this.consumer=con;
    }

    public void addOffset(String topic, int partition, long offset){
        currentOffsets.put(new TopicPartition(topic, partition),new OffsetAndMetadata(offset,"Commit"));
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets(){
        return currentOffsets;
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        System.out.println("Following Partitions are Revoked..");
        for(TopicPartition partition: collection){
            System.out.println(partition.partition() + "," + partition.topic());
        }
        System.out.println("Following Partitions are Committed..");
        for(TopicPartition tp: currentOffsets.keySet())
            System.out.println(tp.partition() + tp.topic());

        consumer.commitSync(currentOffsets);
        currentOffsets.clear();
    }



    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        System.out.println("Following Partitions are Assigned..");

        for(TopicPartition partition: collection){
            System.out.println(partition.partition() + "," + partition.topic());
        }


    }


}
