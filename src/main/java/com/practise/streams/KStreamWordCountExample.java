package com.practise.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;



import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KStreamWordCountExample {

    public static void main(String ...args){

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> textLines = builder
                .stream("wordinput");

        textLines
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, value) -> value)
                .count(Materialized.as("WordCount"))
                .toStream()
                .to("wordoutput", Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();

        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "streams-word-count");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(
                new Thread("streams-shutdown-hook") {
                    @Override
                    public void run() {
                        streams.close();
                        latch.countDown();
                    }
                });
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);



    }
}
