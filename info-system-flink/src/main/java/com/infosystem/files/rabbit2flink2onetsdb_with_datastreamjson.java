// package com.infosystem.files;

// import org.apache.flink.api.common.RuntimeExecutionMode;
// import org.apache.flink.api.common.eventtime.WatermarkStrategy;
// import org.apache.flink.api.common.functions.AggregateFunction;
// import org.apache.flink.api.common.functions.FilterFunction;
// import org.apache.flink.api.common.functions.MapFunction;
// import org.apache.flink.api.common.functions.ReduceFunction;
// import org.apache.flink.api.common.serialization.DeserializationSchema;
// import org.apache.flink.api.common.typeinfo.TypeInformation;
// import org.apache.flink.api.java.functions.KeySelector;
// import org.apache.flink.api.java.tuple.Tuple;
// import org.apache.flink.api.java.tuple.Tuple2;
// import org.apache.flink.api.java.tuple.Tuple3;
// import org.apache.flink.streaming.api.datastream.AllWindowedStream;
// import org.apache.flink.streaming.api.datastream.DataStream;
// import org.apache.flink.streaming.api.datastream.DataStreamSink;
// import org.apache.flink.streaming.api.datastream.KeyedStream;
// import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
// import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
// import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
// import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
// import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
// import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
// import org.apache.flink.streaming.api.watermark.Watermark;
// import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
// import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
// import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
// import org.apache.flink.streaming.api.windowing.time.Time;
// import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
// import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
// import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

// import org.apache.flink.table.api.EnvironmentSettings;
// import org.apache.flink.table.api.Table;
// import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
// import org.apache.flink.types.Row;

// import java.io.IOException;
// import java.nio.charset.StandardCharsets;
// import java.sql.Timestamp;
// import java.text.DateFormat;
// import java.text.SimpleDateFormat;
// import java.util.Arrays;
// import java.util.List;
// import java.util.concurrent.TimeUnit;

// import org.apache.flink.util.Collector;

// import com.ibm.icu.impl.ValidIdentifiers.Datasubtype;
// import com.ibm.icu.impl.locale.LocaleDistance.Data;
// import com.java.infosystem.bestexamples.utilss.RestClient;
// import com.java.infosystem.bestexamples.utilss.eachrow;

// import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
// import org.apache.http.HttpResponse;
// import org.jets3t.service.model.container.ObjectKeyAndVersion;
// import org.w3c.dom.ElementTraversal;

// public class rabbit2flink2onetsdb_with_datastreamjson {

//     public static void main(String[] args) throws Exception {
//         // EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().build();
//         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//         // env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//         StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

//         final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
//                 .setHost("localhost")
//                 .setPort(5672)
//                 .setVirtualHost("/")
//                 .setUserName("guest")
//                 .setPassword("guest")
//                 .build();

//         final DataStream<eachrow> stream = env
//                 .addSource(new RMQSource<eachrow>(
//                         connectionConfig, // config for the RabbitMQ connection
//                         "hello1", // name of the RabbitMQ queue to consume
//                         true, // use correlation ids; can be false if only at-least-once is
//                               // required
//                         new DeserializationSchema<eachrow>() {
//                             @Override
//                             public eachrow deserialize(byte[] message) throws IOException {
//                                 final ObjectMapper objectMapper = new ObjectMapper();
//                                 return objectMapper.readValue(message, eachrow.class);
//                             }

//                             @Override
//                             public TypeInformation<eachrow> getProducedType() {
//                                 return TypeInformation.of(eachrow.class);
//                             }

//                             @Override
//                             public boolean isEndOfStream(eachrow nextElement) {
//                                 // TODO Auto-generated method stub
//                                 return false;
//                             }

//                         })) // deserialization schema to turn messages
//                             // into Java objects
//                 .setParallelism(1);
//         DataStream<eachrow> newStream = stream;

//         DataStreamSink<Tuple2<Long, Double>> avgTh1 = newStream
//                 .filter(new FilterFunction<eachrow>() {
//                     @Override
//                     public boolean filter(eachrow value) throws Exception {
//                         return value.th1 > 0.0;
//                     }
//                 })
//                 .assignTimestampsAndWatermarks(
//                         new BoundedOutOfOrdernessTimestampExtractor<eachrow>(Time.minutes(15)) {
//                             @Override
//                             public long extractTimestamp(eachrow element) {
//                                 return element.getTimeday() * 1000;
//                             }
//                         })
//                 // .keyBy(x -> x.getTimeday() / (24 * 60 * 60 * 1000))
//                 // .timeWindowAll(Time.days(1))
//                 .keyBy(tuple -> tuple.getTimeday() / (24 * 60 * 60 * 1000))
//                 .timeWindow(Time.days(1))
//                 .process(new ProcessWindowFunction<eachrow, Tuple2<Long, Double>, Long, TimeWindow>() {
//                     @Override
//                     public void process(Long key, Context context, Iterable<eachrow> elements,
//                             Collector<Tuple2<Long, Double>> out) {
//                         int count = 0;
//                         double sum = 0;
//                         for (eachrow element : elements) {
//                             count++;
//                             sum += element.th1;
//                         }
//                         System.out.println("Window start: " + context.window().getStart() + ", Window end: "
//                                 + context.window().getEnd() + ", Average: " + count);
//                         out.collect(new Tuple2<>(context.window().getStart(), sum / count));
//                     }
//                 }).print("!").setParallelism(1);

//         // DataStreamSink<Tuple2<Timestamp, Double>> dailyAverage = stream
//         // .filter(new FilterFunction<eachrow>() {

//         // @Override
//         // public boolean filter(eachrow value) throws Exception {
//         // return value.th1 > 0.0;
//         // }
//         // })
//         // .assignTimestampsAndWatermarks(
//         // new BoundedOutOfOrdernessTimestampExtractor<eachrow>(Time.minutes(15)) {
//         // @Override
//         // public long extractTimestamp(eachrow element) {
//         // return element.getTimeday() * 1000;
//         // }
//         // })
//         // .map(new MapFunction<eachrow, Tuple3<Timestamp, Integer, Integer>>() {
//         // @Override
//         // public Tuple3<Timestamp, Integer, Integer> map(eachrow value) throws
//         // Exception {
//         // Timestamp date = new Timestamp((long) value.getTimeday() * 1000);
//         // date.setHours(0);
//         // date.setMinutes(0);
//         // date.setSeconds(0);
//         // return new Tuple3<>(date, (int) value.th1, 1);
//         // }
//         // })
//         // .keyBy(0)
//         // .timeWindow(Time.days(1))
//         // .reduce(new ReduceFunction<Tuple3<Timestamp, Integer, Integer>>() {
//         // @Override
//         // public Tuple3<Timestamp, Integer, Integer> reduce(Tuple3<Timestamp, Integer,
//         // Integer> value1,
//         // Tuple3<Timestamp, Integer, Integer> value2) throws Exception {
//         // return new Tuple3<>(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
//         // }
//         // })
//         // .map(new MapFunction<Tuple3<Timestamp, Integer, Integer>, Tuple2<Timestamp,
//         // Double>>() {
//         // @Override
//         // public Tuple2<Timestamp, Double> map(Tuple3<Timestamp, Integer, Integer>
//         // value) throws Exception {
//         // return new Tuple2<>(value.f0, (double) value.f1 / value.f2);
//         // }
//         // }).print();

//         // // ! THIS IS VERY GOOD
//         // DataStreamSink<Tuple3<Timestamp, Double, Double>> dailyAverage = stream
//         // .filter(new FilterFunction<eachrow>() {
//         // @Override
//         // public boolean filter(eachrow value) throws Exception {
//         // return value.th1 > 0.0;
//         // }
//         // })
//         // .assignTimestampsAndWatermarks(
//         // new BoundedOutOfOrdernessTimestampExtractor<eachrow>(Time.minutes(22)) {
//         // @Override
//         // public long extractTimestamp(eachrow element) {
//         // return element.getTimeday() * 1000;
//         // }
//         // }

//         // )
//         // .keyBy(x -> x.getTimeday() / (24 * 60 * 60 * 1000))
//         // .timeWindow(Time.days(1))
//         // // .timeWindowAll(Time.days(1))
//         // .aggregate(
//         // new AggregateFunction<eachrow, Tuple3<Timestamp, Double, Integer>,
//         // Tuple3<Timestamp, Double, Double>>() {
//         // @Override
//         // public Tuple3<Timestamp, Double, Integer> createAccumulator() {
//         // return new Tuple3<>(null, 0.0, 0);
//         // }

//         // @Override
//         // public Tuple3<Timestamp, Double, Integer> add(eachrow value,
//         // Tuple3<Timestamp, Double, Integer> accumulator) {
//         // Timestamp date = new Timestamp((long) (value.getTimeday()) * 1000); // +
//         // // +86400
//         // // date.setHours(0);
//         // // date.setMinutes(0);
//         // return new Tuple3<>(date, accumulator.f1 + value.th1, accumulator.f2 + 1);
//         // }

//         // @Override
//         // public Tuple3<Timestamp, Double, Double> getResult(
//         // Tuple3<Timestamp, Double, Integer> accumulator) {
//         // return new Tuple3<>(accumulator.f0, accumulator.f1, (double) accumulator.f2);
//         // }

//         // @Override
//         // public Tuple3<Timestamp, Double, Integer> merge(Tuple3<Timestamp, Double,
//         // Integer> a,
//         // Tuple3<Timestamp, Double, Integer> b) {
//         // return new Tuple3<>(a.f0, a.f1 + b.f1, a.f2 + b.f2);
//         // }
//         // })
//         // .print("Main---").setParallelism(1);
//         // !UNTIL HERE
//         // DataStreamSink<Row> runningAverage = stream
//         // .map(new MapFunction<eachrow, Tuple2<Integer, Integer>>() {
//         // int count = 1;
//         // int sum = 0;

//         // @Override
//         // public Tuple2<Integer, Integer> map(eachrow value) throws Exception {
//         // sum += value.th1;
//         // return new Tuple2<>(sum, count++);
//         // }
//         // })
//         // .keyBy(0)
//         // .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
//         // @Override
//         // public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1,
//         // Tuple2<Integer, Integer> value2) throws Exception {
//         // return new Tuple2<>(value1.f0 + value2.f0, value1.f1 + value2.f1);
//         // }
//         // })
//         // .map(new MapFunction<Tuple2<Integer, Integer>, Row>() {
//         // @Override
//         // public Row map(Tuple2<Integer, Integer> value) throws Exception {
//         // return Row.of(value.f0, value.f1, (double) value.f0 / value.f1);
//         // }
//         // }).print("test");
//         // SingleOutputStreamOperator<Integer> asda = newStream
//         // .keyBy(event -> event.day)
//         // .window(GlobalWindows.create())
//         // // .reduce(new ReduceFunction<eachrow>() {
//         // // public eachrow reduce(eachrow value1, eachrow value2) throws Exception {
//         // // long total = 0;
//         // // total = (long) (value1.th1 + value2.th2);
//         // // return new eachrow(value1.timeday, total, Math.round(value2.th2), total,
//         // 0,
//         // // 0,
//         // // 0, 0,
//         // // 0, 0, 0);

//         // // }
//         // // })
//         // // .map(new MapFunction<eachrow, Object>() {
//         // // @Override
//         // // public Row map(eachrow value) throws Exception {
//         // // return Row.of(value.getDay(), value.th1); // our datasource has value of
//         // // Integers, we double
//         // // // these integers
//         // // }
//         // // })
//         // .aggregate(new AggregateFunction<eachrow, Integer, Integer>() {
//         // @Override
//         // public Integer createAccumulator() {
//         // return 0;
//         // }

//         // @Override
//         // public Integer add(eachrow value, Integer accumulator) {
//         // return accumulator + (int) value.th1;
//         // }

//         // @Override
//         // public Integer getResult(Integer accumulator) {
//         // return accumulator;
//         // }

//         // @Override
//         // public Integer merge(Integer a, Integer b) {
//         // System.out.print("asss");
//         // return a + b;
//         // }
//         // });

//         newStream.filter(new FilterFunction<eachrow>() {

//             @Override
//             public boolean filter(eachrow value) throws Exception {
//                 return value.th1 > 30.0;
//             }
//         });

//         // DataStream<eachrow> testdDataStream = newStream
//         // .filter(new FilterFunction<eachrow>() {
//         // @Override
//         // public boolean filter(eachrow value) throws Exception {
//         // return value.th1 > 0.0;
//         // }
//         // })
//         // .assignTimestampsAndWatermarks(
//         // WatermarkStrategy.<eachrow>forMonotonousTimestamps()
//         // .withTimestampAssigner(((event, t) -> event.timeday)));

//         // DataStreamSink<Object> hourlyTips = testdDataStream
//         // .keyBy((eachrow fare) -> fare.day)
//         // .map(new MapFunction<eachrow, Object>() {
//         // @Override
//         // public String map(eachrow value) throws Exception {
//         // return value.day; // our datasource has value of Integers, we double these
//         // integers
//         // }
//         // }).print();
//         // .window(TumblingEventTimeWindows.of(Time.seconds()))
//         // .process(new ProcessWindowFunction<eachrow, Row, String, TimeWindow>() {
//         // @Override
//         // public void process(
//         // String key,
//         // Context context,
//         // Iterable<eachrow> fares,
//         // Collector<Row> out) {

//         // float sumOfTips = 0F;
//         // for (eachrow f : fares) {
//         // System.out.print(" --- " + f.day);
//         // sumOfTips += f.th1;
//         // }
//         // System.out.print("\n");
//         // System.out.print("\n");
//         // System.out.print("\n");
//         // out.collect(Row.of(context.window().getEnd(), key, sumOfTips));
//         // }
//         // });
//         // .maxBy("th1");

//         // hourlyTips.map(new MapFunction<Row, Object>() {
//         // @Override
//         // public Row map(Row value) throws Exception {
//         // return value; // our datasource has value of Integers, we double these
//         // integers
//         // }
//         // }).print();

//         // DataStream<eachrow> eachColorStream = newStream
//         // .keyBy(new KeySelector<eachrow, Double>() {
//         // public Double getKey(eachrow value) {
//         // return value.th1;
//         // }

//         // })
//         // // .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(60 * 15)))
//         // .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(2)))
//         // .reduce(new ReduceFunction<eachrow>() {
//         // public eachrow reduce(eachrow value1, eachrow value2) throws Exception {
//         // return new eachrow(0, value1.th1, value2.th1, 0, 0, 0, 0, 0,
//         // 0, 0, 0);
//         // }
//         // });
//         // eachColorStream.map(new MapFunction<eachrow, Object>() {
//         // @Override
//         // public eachrow map(eachrow value) throws Exception {
//         // return value;// our datasource has value of Integers, we double these
//         // integers
//         // }
//         // }).print();

//         System.out.println("------start-------");

//         Table ta = tEnv.fromDataStream(stream);
//         Table tableforStream = tEnv.fromDataStream(stream);
//         // tableforStream.printSchema();
//         // eachColorStream.print().getTransformation().getId();

//         // tableforStream.execute().print();

//         Table tableEvents = tEnv.sqlQuery("SELECT * FROM " + tableforStream + " WHERE th1>0 AND th2>0");
//         Table tableLateEvents = tEnv.sqlQuery("SELECT * FROM " + tableforStream + " WHERE th1=0 AND th2=0");

//         // tableEvents.printSchema();
//         DataStream<Row> tDataStream = tEnv.toDataStream(tableEvents);

//         // tableEvents.execute().print();

//         List<String> sensors = Arrays.asList("th1", "th2", "hvac1", "hvac2", "miac1", "miac2", "etot", "mov1", "w1",
//                 "wtot");

//         String sensor = "th1";

//         // for (String sensor : sensors) {
//         // System.out.println("eppp " + sensor);

//         Table result = tEnv
//                 .sqlQuery("SELECT timeday, " + sensor + " FROM " + tableEvents + " WHERE timeday <= 1731707100");

//         // result.execute();
//         // result.printSchema();
//         // result.execute().print();
//         Table result1 = tEnv.sqlQuery(
//                 "SELECT EXTRACT(DAY FROM to_timestamp(from_unixtime(timeday))) AS day1, ROUND(SUM("
//                         + sensor
//                         + "),0) AS SUMM FROM "
//                         + tableEvents + " GROUP BY EXTRACT(DAY FROM to_timestamp(from_unixtime(timeday)))");

//         // result1.printSchema();
//         // result1.execute();
//         // result1.execute().print();

//         // DataStream<Row> dataStream = tEnv.toDataStream(result1);
//         // dataStream.collectAsync();
//         // dataStream.print();
//         // dataStream.executeAndCollect();

//         // ! auto kanei mapping kathe seira kai na thn grafei sthn bash
//         // ! meso apo thn writeToOpenTSDB
//         // laststream.map(
//         // new MapFunction<Tuple2<Long, Double>, Object>() {
//         // @Override
//         // public Object map(Tuple2<Long, Double> answerRow) throws Exception {
//         // // System.out.println("yesss it is= " + answerRow + "\n");

//         // writeToOpenTSDB(answerRow, sensor);
//         // return answerRow;
//         // }
//         // });

//         // ! auto ta stelnei piso se ena queue
//         // * DEN NOMIZO NA XREIASTEI POTE
//         // stream.addSink(new RMQSink<String>(
//         // connectionConfig, // config for the RabbitMQ connection
//         // "hello2", // name of the RabbitMQ queue to send messages to
//         // new SimpleStringSchema()));

//         // env.execute();
//         // stream.print();
//         // }
//         env.execute("just see what happen");
//     }

//     private static void writeToOpenTSDB(Tuple2<Long, Double> answerRow, String sensor) {
//         // System.out.println("answerrow = " + answerRow);
//         // System.out.println("sensor = " + sensor);

//         RestClient client = new RestClient();
//         String protocol = "http";
//         String host = "localhost";
//         String port = "4242";
//         String path = "/api/put";
//         byte[] eventBody = null;
//         // Double temperature = new Double(answerRow.getField(1).toString());
//         Double temperature = answerRow.f1;
//         String sensorName = sensor;
//         // long time = System.currentTimeMillis() / 1000 - 360 * 60 * 60 * 24; //
//         // now-120days
//         // Integer time = (Integer) answerRow.getField(0);
//         Long time = answerRow.f0;
//         System.out.println(time);
//         time = time + 60 * 60 * 24 * 1000;

//         // String msg = "{\"metric\": \"temperature\", \"timestamp\": %s, \"value\": %s,
//         // \"tags\": {\"%s\" : \"%s\" }}";
//         String msg = "{\"metric\": \"%s\", \"timestamp\": %s, \"value\": %s, \"tags\": {\"%s\" : \"%s\" }}";
//         DateFormat dF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
//         String jsonEvent = String.format(msg, sensorName, (time), temperature, sensorName, sensorName);
//         eventBody = jsonEvent.getBytes();
//         String convertedMsg = new String(eventBody, StandardCharsets.UTF_8);
//         System.out.println("Final Message: " + convertedMsg);

//         if (eventBody != null && eventBody.length > 0) {
//             // HttpResponse res = client.publishToOpenTSDB(protocol, host, port, path,
//             // convertedMsg);
//             // System.out.println("Response :" + res.getStatusLine());
//             ;
//         }
//     }
// }