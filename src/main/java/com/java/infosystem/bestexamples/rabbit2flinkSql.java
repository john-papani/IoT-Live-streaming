package com.java.infosystem.bestexamples;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import java.util.Arrays;

import org.apache.http.HttpResponse;

public class rabbit2flinkSql {

    public static void main(String[] args) throws Exception {

        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost")
                .setPort(5672)
                .setVirtualHost("/")
                .setUserName("guest")
                .setPassword("guest")
                .build();

        final DataStream<String> stream = env
                .addSource(new RMQSource<String>(
                        connectionConfig, // config for the RabbitMQ connection
                        "hello1", // name of the RabbitMQ queue to consume
                        true, // use correlation ids; can be false if only at-least-once is
                              // required
                        new SimpleStringSchema())) // deserialization schema to turn messages
                                                   // into Java objects
                .setParallelism(1);

        // ! diko moy paradeigma gia learning
        // final DataStream<Numbers> orderA = env.fromCollection(
        //         Arrays.asList(
        //                 new Numbers(1, 2, 3),
        //                 new Numbers(1, 2, 4),
        //                 new Numbers(4, 5, 2),
        //                 new Numbers(2, 5, 3),
        //                 new Numbers(7, 7, 3),
        //                 new Numbers(1, 1, 1)));

        // final Table tableA = tableEnv.fromDataStream(orderA);

        // tableA.printSchema();
        // Table counts = tableA
        //         .groupBy("a")
        //         .select("*");
        // counts.execute().print();
        // ! END OF MY EXAMPLE

        // ! auto ta stelnei piso se ena queue
        // stream.addSink(new RMQSink<String>(
        // connectionConfig, // config for the RabbitMQ connection
        // "hello2", // name of the RabbitMQ queue to send messages to
        // new SimpleStringSchema()));

        // env.execute();
        stream.print();
        env.execute("just see what happen");
    }

    public static class Numbers {
        public int a;
        public int b;
        public int c;

        // for POJO detection in DataStream API
        public Numbers() {
        }

        // for structured type detection in Table API
        public Numbers(int a, int b, int c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        @Override
        public String toString() {
            return "Numbers{"
                    + "a="
                    + a
                    + ", b='"
                    + b
                    + '\''
                    + ", c="
                    + c
                    + '}';
        }
    }

}
