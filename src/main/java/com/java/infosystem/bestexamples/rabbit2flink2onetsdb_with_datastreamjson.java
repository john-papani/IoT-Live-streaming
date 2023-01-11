package com.java.infosystem.bestexamples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import com.java.infosystem.utils.RestClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import com.java.infosystem.bestexamples.utilss.eachrow;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;

public class rabbit2flink2onetsdb_with_datastreamjson {

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

        final DataStream<eachrow> stream = env
                .addSource(new RMQSource<eachrow>(
                        connectionConfig, // config for the RabbitMQ connection
                        "hello1", // name of the RabbitMQ queue to consume
                        true, // use correlation ids; can be false if only at-least-once is
                              // required
                        new DeserializationSchema<eachrow>() {
                            @Override
                            public eachrow deserialize(byte[] message) throws IOException {
                                final ObjectMapper objectMapper = new ObjectMapper();
                                return objectMapper.readValue(message, eachrow.class);
                            }

                            @Override
                            public TypeInformation<eachrow> getProducedType() {
                                return TypeInformation.of(eachrow.class);
                            }

                            @Override
                            public boolean isEndOfStream(eachrow nextElement) {
                                // TODO Auto-generated method stub
                                return false;
                            }

                        })) // deserialization schema to turn messages
                            // into Java objects
                .setParallelism(1);

        // stream.print().getTransformation().getId();
        final Table tableforStream = tEnv.fromDataStream(stream);
        // tableforStream.printSchema();

        String sensor = "th1";
        Table result = tEnv.sqlQuery(
                "SELECT timeday, " + sensor + " FROM " + tableforStream + " WHERE th1 > 20 ");

        // result.execute().print();
        // result.printSchema();

        DataStream<Row> dataStream = tEnv.toAppendStream(result, Row.class);

        // ! auto kanei mapping kathe seira kai na thn grafei sthn bash
        // ! meso apo thn writeToOpenTSDB
        dataStream.map(
                new MapFunction<Row, Object>() {
                    @Override
                    public Object map(Row answerRow) throws Exception {
                        System.out.println("yesss it is= " + answerRow + "\n");

                        writeToOpenTSDB(answerRow);
                        return answerRow;
                    }
                }).print();

        // ! auto ta stelnei piso se ena queue
        // * DEN NOMIZO NA XREIASTEI POTE
        // stream.addSink(new RMQSink<String>(
        // connectionConfig, // config for the RabbitMQ connection
        // "hello2", // name of the RabbitMQ queue to send messages to
        // new SimpleStringSchema()));

        // env.execute();
        // stream.print();
        env.execute("just see what happen");
    }

    private static void writeToOpenTSDB(Row answerRow) {
        RestClient client = new RestClient();
        String protocol = "http";
        String host = "localhost";
        String port = "4242";
        String path = "/api/put";
        byte[] eventBody = null;
        Double temperature = new Double(answerRow.getField(1).toString());
        String sensorid = "th1";
        // long time = System.currentTimeMillis() / 1000 - 360 * 60 * 60 * 24; //
        // now-120days
        Integer time = (Integer) answerRow.getField(0);

        String msg = "{\"metric\": \"temperature\", \"timestamp\": %s, \"value\": %s, \"tags\": {\"sensor\" : \"%s\" }}";
        DateFormat dF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        String jsonEvent = String.format(msg, (time), temperature, sensorid);
        eventBody = jsonEvent.getBytes();
        String convertedMsg = new String(eventBody, StandardCharsets.UTF_8);
        System.out.println("Final Message: " + convertedMsg);

        if (eventBody != null && eventBody.length > 0) {
            HttpResponse res = client.publishToOpenTSDB(protocol, host, port, path,
                    convertedMsg);
            System.out.println("Response :" + res.getStatusLine());
        }
    }
}
