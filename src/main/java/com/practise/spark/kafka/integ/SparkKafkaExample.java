package com.practise.spark.kafka.integ;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkKafkaExample {

    public static void main(String ...args) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("kafka integration example")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic4")
                .load();
        Dataset<Row> ds =  df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        ds.writeStream().format("console").outputMode("append").start().awaitTermination();


    }
}
