package com.java.infosystem.bestexamples;

import org.apache.commons.math.ode.sampling.StepInterpolator;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.*;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.HBase;
import org.apache.flink.table.descriptors.Json;
import org.apache.hadoop.conf.Configuration;

import com.java.infosystem.constantHbase.*;
import com.java.infosystem.utils.RestClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import org.apache.http.HttpResponse;

public class rabbit2flink2opentsdb {

        public static void main(String[] args) throws Exception {
                EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode()
                                .build();
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

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

                final Table tableforStream = tEnv.fromDataStream(stream).as("id");
                tableforStream.printSchema();
                tableforStream.execute().print();
                // ! diko moy paradeigma gia learning
                // final DataStream<Numbers> orderA = env.fromCollection(
                // Arrays.asList(
                // new Numbers(1, 2, 3),
                // new Numbers(1, 2, 4),
                // new Numbers(4, 5, 2),
                // new Numbers(2, 5, 3),
                // new Numbers(7, 7, 3),
                // new Numbers(1, 1, 1)));

                // final Table tableA = tEnv.fromDataStream(orderA);

                // tableA.printSchema();
                // Table counts = tableA
                // .groupBy("a")
                // .select("*");
                // counts.execute().print();
                // ! END OF MY EXAMPLE

                stream.map(new MapFunction<String, Object>() {
                        @Override
                        public Object map(String string) throws Exception {
                                System.out.println("yesss it is= " + string + "\n");

                                writeToOpenTSDB(string);
                                // writeEventToHbase(string);
                                return string;
                        }
                }).print();
                env.execute("flink learning connectors hbase");

                // ! auto ta stelnei piso se ena queue
                // stream.addSink(new RMQSink<String>(
                // connectionConfig, // config for the RabbitMQ connection
                // "hello2", // name of the RabbitMQ queue to send messages to
                // new SimpleStringSchema()));

                // env.execute();
                // stream.print();
                env.execute("just see what happen");
        }

        public class Numbers {
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

        private static void writeToOpenTSDB(String string) {
                RestClient client = new RestClient();
                String protocol = "http";
                String host = "localhost";
                String port = "4242";
                String path = "/api/put";
                byte[] eventBody = null;
                String temperature = string;
                String sensorid = "fridgeSensor";
                long time = System.currentTimeMillis() / 1000 - 360 * 60 * 60 * 24; // now-120days
                Integer oneday = 3600 * Integer.parseInt(string);

                String msg = "{\"metric\": \"temperature\", \"timestamp\": %s, \"value\": %s, \"tags\": {\"sensor\" : \"%s\" }}";
                DateFormat dF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                String jsonEvent = String.format(msg, (time + oneday), temperature, sensorid);
                eventBody = jsonEvent.getBytes();
                String convertedMsg = new String(eventBody, StandardCharsets.UTF_8);
                System.out.println("Final Message:   " + convertedMsg);
                if (eventBody != null && eventBody.length > 0) {
                        HttpResponse res = client.publishToOpenTSDB(protocol, host, port, path, convertedMsg);
                        System.out.println("Response :" + res.getStatusLine());
                }
        }

}

// {
// Define a new HBase sink for writing to the ITEM_QUERIES table
// Configure our sink to not buffer operations for more than a second (to reduce
// end-to-end latency)
// hbaseSink.setWriteOptions(HBaseWriteOptions.builder()
// .setBufferFlushIntervalMillis(1000)
// .build());
// Add the sink to our query result streamqueryResultStream.addSink(hbaseSink);
// TableEnvironment tableEnv = TableEnvironment.create(settings);
// Table inputTable = tableEnv.fromDataStream(stream).as("name", "score");
// Table orders = tEnv.from("Orders"); // schema (a, b, c, rowtime)

// Table counts = orders
// .groupBy($("a"))
// .select($("a"), $("b").count().as("cnt"));

// // print
// counts.execute().print();
// // DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);
// // tableEnv.toDataStream(resultTable).print();

// Table topN = tableEnv.sqlQuery(
// " CREATE TABLE hTable (" +
// "rowkey INT," +
// "family1 ROW<q1 INT>," +
// "family2 ROW<q2 STRING, q3 BIGINT>," +
// "family3 ROW<q4 DOUBLE, q5 BOOLEAN, q6 STRING>," +
// "PRIMARY KEY (rowkey) NOT ENFORCED" +
// ") WITH (" +
// "'connector' = 'hbase-1.4'," +
// "'table-name' = 'mytable'," +
// "'zookeeper.quorum' = 'localhost:2181'" +
// ")");

// Table inputTable = tableEnv.fromDataStream(stream).as("id");

// inputTable.printSchema();
// register the Table object as a view and query it

// tableEnv.createTemporaryView("InputTable", inputTable);
// Table resultTable = tableEnv.sqlQuery("SELECT f0 FROM InputTable");
// HBaseSinkFunction<> hbaseSink = new HBaseSinkFunction<>( hTable, null, null,
// 0, 0, 0)
// hbaseSink.setWriteOptions(HBaseWriteOptions.builder()
// .setBufferFlushIntervalMillis(1000)
// .build());

// ************************************************************
// // datastream to table
// final Table tableFromStream = tEnv.fromDataStream(stream);
// String sink_ddl = ("CREATE TABLE hTable (" +
// "a INT," +
// "b INT," +
// "c INT," +
// "PRIMARY KEY (a) NOT ENFORCED" +
// ") WITH (" +
// "'connector' = 'hbase-1.4'," +
// "'table-name' = 'mytable'," +
// "'zookeeper.quorum' = 'localhost:2181'" +
// ")");
// TableResult tt = tEnv.executeSql(sink_ddl);
// tt.print();
// String newSql = ("INSERT INTO hTable VALUES (1, 2, 3),(1, 2, 4),(4, 5, 2),(2,
// 5, 3),(7, 7, 3),(1, 1, 1)");
// TableResult tt1 = tEnv.executeSql(newSql);

// tt1.print();
// tableFromStream.execute().print();
// }