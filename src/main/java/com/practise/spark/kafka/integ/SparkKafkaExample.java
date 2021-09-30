package com.practise.spark.kafka.integ;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkKafkaExample {

    public static void main(String ...args) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .master("local[1]")
                .appName("kafka integration example")
                .getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic2")
                .load();
        df.writeStream().format("console").outputMode("append").start().awaitTermination();


    }
}
