package com.infosystem.files;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;
import org.apache.http.HttpResponse;

import com.infosystem.files.utils.RestClient;
import com.infosystem.files.utils.eachrow;

public class all_aggregation {

    public static void main(String[] args) throws Exception {

        Configuration cfg = new Configuration();
        int defaultLocalParallelism = Runtime.getRuntime().availableProcessors();
        cfg.setString("taskmanager.memory.network.max", "1gb");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(defaultLocalParallelism,
                cfg);
        System.out.println("--------start---------");

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
                        "dataQueue", // name of the RabbitMQ queue to consume
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

        DataStream<eachrow> rawData = stream.filter(new FilterFunction<eachrow>() {
            @Override
            public boolean filter(eachrow value) throws Exception {
                return value.th1 > 0.0;
            }
        });

        DataStream<Tuple2<Long, Double>> streamWithTwoDaysLateEvents = stream
                .keyBy(event -> event.getTimeday() / (24 * 60 * 60 * 1000))
                .process(new KeyedProcessFunction<Long, eachrow, Tuple2<Long, Double>>() {
                    private Long previousTimestamp = null;

                    @Override
                    public void processElement(eachrow event, Context ctx,
                            Collector<Tuple2<Long, Double>> out) {
                        if (previousTimestamp != null
                                && previousTimestamp - event.getTimeday() * 1000 <= 2 * 24 * 60 * 60 * 1000) {
                            out.collect(new Tuple2<>(event.getTimeday(), event.w1));
                        }
                        previousTimestamp = event.getTimeday() * 1000;
                    }
                }).setParallelism(1);

        // * ------------------------ START = RAW DATA ---------------------------------
        SingleOutputStreamOperator<Tuple2<Long, Double>> onlyTh1 = rawData
                .map(new MapFunction<eachrow, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(eachrow value) {
                        // Perform the transformation and return the new element
                        return new Tuple2<Long, Double>(value.getTimeday() * 1000, (double) value.th1);
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> onlyTh2 = rawData
                .map(new MapFunction<eachrow, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(eachrow value) {
                        // Perform the transformation and return the new element
                        return new Tuple2<Long, Double>(value.getTimeday() * 1000, (double) value.th2);
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> onlyHvac1 = rawData
                .map(new MapFunction<eachrow, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(eachrow value) {
                        // Perform the transformation and return the new element
                        return new Tuple2<Long, Double>(value.getTimeday() * 1000, (double) value.hvac1);
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> onlyHvac2 = rawData
                .map(new MapFunction<eachrow, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(eachrow value) {
                        // Perform the transformation and return the new element
                        return new Tuple2<Long, Double>(value.getTimeday() * 1000, (double) value.hvac2);
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> onlyMiac1 = rawData
                .map(new MapFunction<eachrow, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(eachrow value) {
                        // Perform the transformation and return the new element
                        return new Tuple2<Long, Double>(value.getTimeday() * 1000, (double) value.miac1);
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> onlyMiac2 = rawData
                .map(new MapFunction<eachrow, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(eachrow value) {
                        // Perform the transformation and return the new element
                        return new Tuple2<Long, Double>(value.getTimeday() * 1000, (double) value.miac2);
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> onlyEtot = rawData
                .map(new MapFunction<eachrow, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(eachrow value) {
                        // Perform the transformation and return the new element
                        return new Tuple2<Long, Double>(value.getTimeday() * 1000, (double) value.etot);
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> onlyMov1 = rawData
                .map(new MapFunction<eachrow, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(eachrow value) {
                        // Perform the transformation and return the new element
                        return new Tuple2<Long, Double>(value.getTimeday() * 1000, (double) value.etot);
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> onlyW1 = streamWithTwoDaysLateEvents
                .map(new MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
                        // Perform the transformation and return the new element
                        return new Tuple2<Long, Double>(value.f0, (double) value.f1);
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> onlyWtot = rawData
                .map(new MapFunction<eachrow, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(eachrow value) {
                        // Perform the transformation and return the new element
                        return new Tuple2<Long, Double>(value.getTimeday() * 1000, (double) value.wtot);
                    }
                });

        // * ------------------------ END = RAW DATA ---------------------------------
        // * ------------------------ START = LATE EVENT DATA
        // ---------------------------------

        DataStream<Tuple2<Long, Double>> twoDaysLateStream = stream
                .keyBy(event -> event.getTimeday() / (24 * 60 * 60 * 1000))
                .process(new KeyedProcessFunction<Long, eachrow, Tuple2<Long, Double>>() {
                    private Long previousTimestamp = null;

                    @Override
                    public void processElement(eachrow event, Context ctx,
                            Collector<Tuple2<Long, Double>> out) {
                        if (previousTimestamp != null
                                && previousTimestamp - event.getTimeday() * 1000 == 2 * 24 * 60 * 60 * 1000) {
                            out.collect(new Tuple2<>(event.getTimeday(), event.w1));
                        }
                        previousTimestamp = event.getTimeday() * 1000;
                    }
                });

        DataStream<Tuple2<Long, Double>> tenDaysLateStream = stream
                .keyBy(event -> event.getTimeday() / (24 * 60 * 60 * 1000))
                .process(new KeyedProcessFunction<Long, eachrow, Tuple2<Long, Double>>() {
                    private Long previousTimestamp = null;

                    @Override
                    public void processElement(eachrow event, Context ctx,
                            Collector<Tuple2<Long, Double>> out) {
                        if (previousTimestamp != null
                                && previousTimestamp - event.getTimeday() * 1000 >= 7 * 24 * 60 * 60 * 1000) {
                            out.collect(new Tuple2<>(event.getTimeday(), event.w1));
                        }
                        previousTimestamp = event.getTimeday() * 1000;
                    }
                });
        // * ------------------------ END = LATE EVENT DATA

        // * ------------------------ START = AGGREGATIONS DATA

        WindowedStream<eachrow, Long, TimeWindow> oneDayWindowedStream = rawData
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<eachrow>(Time.seconds(1)) {
                            @Override
                            public long extractTimestamp(eachrow element) {
                                return element.getTimeday() * 1000;
                            }
                        })
                .keyBy(tuple -> tuple.getTimeday() / (24 * 60 * 60 * 1000))
                .timeWindow(Time.days(1));

        SingleOutputStreamOperator<Tuple2<Long, Double>> avgTh1 = oneDayWindowedStream
                .process(new ProcessWindowFunction<eachrow, Tuple2<Long, Double>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<eachrow> elements,
                            Collector<Tuple2<Long, Double>> out) {
                        int count = 0;
                        double sum = 0;
                        for (eachrow element : elements) {
                            count++;
                            sum += element.th1;
                        }
                        out.collect(new Tuple2<>(context.window().getStart(), sum / count));
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> avgTh2 = oneDayWindowedStream
                .process(new ProcessWindowFunction<eachrow, Tuple2<Long, Double>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<eachrow> elements,
                            Collector<Tuple2<Long, Double>> out) {
                        int count = 0;
                        double sum = 0;
                        for (eachrow element : elements) {
                            count++;
                            sum += element.th2;
                        }
                        out.collect(new Tuple2<>(context.window().getStart(), sum / count));
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> sumHvac1 = oneDayWindowedStream
                .process(new ProcessWindowFunction<eachrow, Tuple2<Long, Double>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<eachrow> elements,
                            Collector<Tuple2<Long, Double>> out) {
                        double sum = 0;
                        for (eachrow element : elements) {
                            sum += element.hvac1;
                        }
                        out.collect(new Tuple2<>(context.window().getStart(), sum));
                    }
                }).setParallelism(1);

        SingleOutputStreamOperator<Tuple2<Long, Double>> sumHvac2 = oneDayWindowedStream
                .process(new ProcessWindowFunction<eachrow, Tuple2<Long, Double>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<eachrow> elements,
                            Collector<Tuple2<Long, Double>> out) {
                        double sum = 0;
                        for (eachrow element : elements) {
                            sum += element.hvac2;
                        }
                        out.collect(new Tuple2<>(context.window().getStart(), sum));
                    }
                }).setParallelism(1);

        SingleOutputStreamOperator<Tuple2<Long, Double>> sumMiac1 = oneDayWindowedStream
                .process(new ProcessWindowFunction<eachrow, Tuple2<Long, Double>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<eachrow> elements,
                            Collector<Tuple2<Long, Double>> out) {
                        double sum = 0;
                        for (eachrow element : elements) {
                            sum += element.miac1;
                        }
                        out.collect(new Tuple2<>(context.window().getStart(), sum));
                    }
                }).setParallelism(1);

        SingleOutputStreamOperator<Tuple2<Long, Double>> sumMiac2 = oneDayWindowedStream
                .process(new ProcessWindowFunction<eachrow, Tuple2<Long, Double>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<eachrow> elements,
                            Collector<Tuple2<Long, Double>> out) {
                        double sum = 0;
                        for (eachrow element : elements) {
                            sum += element.miac2;
                        }
                        out.collect(new Tuple2<>(context.window().getStart(), sum));
                    }
                }).setParallelism(1);

        SingleOutputStreamOperator<Tuple2<Long, Double>> maxEtot = oneDayWindowedStream
                .process(new ProcessWindowFunction<eachrow, Tuple2<Long, Double>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<eachrow> elements,
                            Collector<Tuple2<Long, Double>> out) {
                        double max = -1.0;
                        for (eachrow element : elements) {
                            if (element.etot > max)
                                max = element.etot;
                        }
                        out.collect(new Tuple2<>(context.window().getStart(), max));
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> sumMov1 = oneDayWindowedStream
                .process(new ProcessWindowFunction<eachrow, Tuple2<Long, Double>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<eachrow> elements,
                            Collector<Tuple2<Long, Double>> out) {
                        double sum = 0;
                        for (eachrow element : elements) {
                            sum += element.mov1;
                        }
                        out.collect(new Tuple2<>(context.window().getStart(), sum));
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> sumW1 = streamWithTwoDaysLateEvents
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Tuple2<Long, Double>>(Time.days(1)) {
                            @Override
                            public long extractTimestamp(Tuple2<Long, Double> element) {
                                return element.f0 * 1000;
                            }
                        })
                .keyBy(tuple -> tuple.f0 / (24 * 60 * 60 * 1000))
                .timeWindow(Time.days(1))
                .process(new ProcessWindowFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<Tuple2<Long, Double>> elements,
                            Collector<Tuple2<Long, Double>> out) {
                        double sum = 0;
                        for (Tuple2<Long, Double> element : elements) {
                            sum += element.f1;
                        }
                        out.collect(new Tuple2<>(context.window().getStart(), sum));
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> maxWtot = oneDayWindowedStream
                .process(new ProcessWindowFunction<eachrow, Tuple2<Long, Double>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<eachrow> elements,
                            Collector<Tuple2<Long, Double>> out) {
                        double max = -1.0;
                        for (eachrow element : elements) {
                            if (element.wtot > max)
                                max = element.wtot;
                        }
                        out.collect(new Tuple2<>(context.window().getStart(), max));
                    }
                });

        // * ------------------------ END = AGGREGATIONS DATA

        // * ------------------------ START AggDayDiff[y] --------------

        DataStream<Tuple2<Long, Double>> diffMaxEtot = maxEtot
                .map(new MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    private Tuple2<Long, Double> previous = null;

                    @Override
                    public Tuple2<Long, Double> map(Tuple2<Long, Double> value) throws Exception {
                        if (previous == null) {
                            previous = value;
                            return Tuple2.of(value.f0, 0.0);
                        } else {
                            Tuple2<Long, Double> current = value;
                            Double result = current.f1 - previous.f1;
                            previous = current;
                            return Tuple2.of(current.f0, result);
                        }
                    }
                })
                .keyBy(0) // Key by the timestamp field
                .map(new MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(Tuple2<Long, Double> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1);
                    }
                }).setParallelism(1);

        DataStream<Tuple2<Long, Double>> diffMaxWtot = maxWtot
                .map(new MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    private Tuple2<Long, Double> previous = null;

                    @Override
                    public Tuple2<Long, Double> map(Tuple2<Long, Double> value) throws Exception {
                        if (previous == null) {
                            previous = value;
                            return Tuple2.of(value.f0, 0.0);
                        } else {
                            Tuple2<Long, Double> current = value;
                            Double result = current.f1 - previous.f1;
                            previous = current;
                            return Tuple2.of(current.f0, result);
                        }
                    }
                })
                .keyBy(0) // Key by the timestamp field
                .map(new MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> map(Tuple2<Long, Double> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1);
                    }
                });

        // * ------------------------ END AggDayDiff[y] --------------

        // * ------------------------ START AggDayRest[y] 1--------------
        // ? AggDayDiff[Etot] - AggDay[HVAC1] - AggDay[HVAC2] - AggDay[MiAC1]
        // ? -AggDay[MiAC2]

        DataStream<Tuple2<Long, Double>> lastSumStream = sumHvac1
                .union(sumHvac2, sumMiac1, sumMiac2)
                .keyBy(0)
                .sum(1)
                .setParallelism(1);

        DataStream<Tuple2<Long, Double>> aggDayEtot_Hvac_Miac = lastSumStream
                .keyBy(0)
                .connect(diffMaxEtot.keyBy(0))
                .flatMap(new CoFlatMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    Tuple2<Long, Double> sum = null;

                    @Override
                    public void flatMap1(Tuple2<Long, Double> value, Collector<Tuple2<Long, Double>> out)
                            throws Exception {
                        if (sum == null) {
                            out.collect(value);
                        } else {
                            out.collect(Tuple2.of(value.f0, sum.f1 - value.f1));
                        }
                    }

                    @Override
                    public void flatMap2(Tuple2<Long, Double> value, Collector<Tuple2<Long, Double>> out)
                            throws Exception {
                        sum = value;
                    }
                });

        // aggDayEtot_Hvac_Miac.print("1").setParallelism(1);

        // *------------------------ END AggDayRest[y] 1 --------------

        // *------------------------ START AggDayRest[y] 2--------------
        // ? AggDayDiff[Wto] – AggDay[W1]

        DataStream<Tuple2<Long, Double>> aggDiffWto_DayW1 = diffMaxWtot.keyBy(0).connect(sumW1.keyBy(0))
                .flatMap(new CoFlatMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    Map<Long, Double> values = new HashMap<>();

                    @Override
                    public void flatMap1(Tuple2<Long, Double> value1, Collector<Tuple2<Long, Double>> out)
                            throws Exception {
                        Double value2 = values.get(value1.f0);
                        if (value2 != null) {
                            out.collect(new Tuple2<>(value1.f0, value1.f1 - value2));
                            values.remove(value1.f0);
                        } else {
                            values.put(value1.f0, value1.f1);
                        }
                    }

                    @Override
                    public void flatMap2(Tuple2<Long, Double> value2, Collector<Tuple2<Long, Double>> out)
                            throws Exception {
                        Double value1 = values.get(value2.f0);
                        if (value1 != null) {
                            out.collect(new Tuple2<>(value2.f0, value1 - value2.f1));
                            values.remove(value2.f0);
                        } else {
                            values.put(value2.f0, value2.f1);
                        }
                    }
                });
        // * ------------------------ END AggDayRest[y] 2 --------------

        // ! auto kanei mapping kathe seira kai na thn grafei sthn bash
        // ! meso apo thn writeToOpenTSDB

        // * ------------------------ SEND ALL DATA TO OPENTSDB --------------

        List<SingleOutputStreamOperator<Tuple2<Long, Double>>> rawDataList = new ArrayList<>();
        rawDataList.add(onlyTh1);
        rawDataList.add(onlyTh2);
        rawDataList.add(onlyHvac1);
        rawDataList.add(onlyHvac2);
        rawDataList.add(onlyMiac1);
        rawDataList.add(onlyMiac2);
        rawDataList.add(onlyEtot);
        rawDataList.add(onlyMov1);
        rawDataList.add(onlyW1);
        rawDataList.add(onlyWtot);

        List<SingleOutputStreamOperator<Tuple2<Long, Double>>> aggList = new ArrayList<>();
        aggList.add(avgTh1);
        aggList.add(avgTh2);
        aggList.add(sumHvac1);
        aggList.add(sumHvac2);
        aggList.add(sumMiac1);
        aggList.add(sumMiac2);
        aggList.add(maxEtot);
        aggList.add(sumMov1);
        aggList.add(sumW1);
        aggList.add(maxWtot);

        List<DataStream<Tuple2<Long, Double>>> aggDayList = new ArrayList<>();
        aggDayList.add(diffMaxEtot);
        aggDayList.add(diffMaxWtot);
        aggDayList.add(aggDiffWto_DayW1);
        aggDayList.add(aggDayEtot_Hvac_Miac);

        List<DataStream<Tuple2<Long, Double>>> lateEventList = new ArrayList<>();
        lateEventList.add(twoDaysLateStream);
        lateEventList.add(tenDaysLateStream);

        List<String> sensors = Arrays.asList("th1", "th2", "hvac1", "hvac2", "miac1",
                "miac2", "etot", "mov1", "w1",
                "wtot");
        List<String> aggSensors = Arrays.asList("avgTh1", "avgTh2", "sumHvac1",
                "sumHvac2", "sumMiac1", "sumMiac2", "maxEtot", "sumMov1", "sumW1",
                "maxWtot");
        List<String> aggDaySensors = Arrays.asList("diffMaxEtot", "diffMaxWtot", "aggDiffWto_DayW1",
                "aggDayEtot_Hvac_Miac");
        List<String> lateEventSensors = Arrays.asList("twoDaysLateStream", "tenDaysLateStream");

        for (int i = 0; i < rawDataList.size(); i++) {
            SingleOutputStreamOperator<Tuple2<Long, Double>> rawDatastream = rawDataList.get(i);
            String sensor = sensors.get(i);
            rawDatastream.map(
                    new MapFunction<Tuple2<Long, Double>, Object>() {
                        @Override
                        public Object map(Tuple2<Long, Double> answerRow) throws Exception {
                            writeToOpenTSDB(answerRow, sensor, false);
                            return answerRow;
                        }
                    });
        }

        for (int i = 0; i < aggList.size(); i++) {
            SingleOutputStreamOperator<Tuple2<Long, Double>> agg = aggList.get(i);
            String sensor = aggSensors.get(i);
            agg.map(
                    new MapFunction<Tuple2<Long, Double>, Object>() {
                        @Override
                        public Object map(Tuple2<Long, Double> answerRow) throws Exception {
                            writeToOpenTSDB(answerRow, sensor, true);
                            return answerRow;
                        }
                    });
        }

        for (int i = 0; i < aggDayList.size(); i++) {
            DataStream<Tuple2<Long, Double>> aggDayDiffstream = aggDayList.get(i);
            String sensor = aggDaySensors.get(i);
            aggDayDiffstream.map(
                    new MapFunction<Tuple2<Long, Double>, Object>() {
                        @Override
                        public Object map(Tuple2<Long, Double> answerRow) throws Exception {
                            writeToOpenTSDB(answerRow, sensor, true);
                            return answerRow;
                        }
                    });
        }

        for (int i = 0; i < lateEventList.size(); i++) {
            DataStream<Tuple2<Long, Double>> lateEventItem = lateEventList.get(i);
            String sensor = lateEventSensors.get(i);
            lateEventItem.map(
                    new MapFunction<Tuple2<Long, Double>, Object>() {
                        @Override
                        public Object map(Tuple2<Long, Double> answerRow) throws Exception {
                            writeToOpenTSDB(answerRow, sensor, false);
                            return answerRow;
                        }
                    });
        }

        // final String URL = "ws://localhost:3000/api/live/push/";
        // final String HEADER_NAME = "Authorization";
        // final String token =
        // "eyJrIjoiU3cwcDZsR2ZWN2JUUHJKbk1pT1pVbERGc1FuUUJXMDUiLCJuIjoid2Vic29ja2V0IiwiaWQiOjF9";
        // final String HEADER_VALUE = "Bearer ";
        // DataStream<Object> tasa = tenDaysLateStream.map(tuple -> {
        // OkHttpClient client = new OkHttpClient.Builder()
        // .readTimeout(0, TimeUnit.MILLISECONDS)
        // .build();

        // Request request = new Request.Builder()
        // .url(URL)
        // .addHeader(HEADER_NAME, HEADER_VALUE
        // +
        // "eyJrIjoiU3cwcDZsR2ZWN2JUUHJKbk1pT1pVbERGc1FuUUJXMDUiLCJuIjoid2Vic29ja2V0IiwiaWQiOjF9")
        // .build();

        // client.newWebSocket(request, new WebSocketListener() {
        // @Override
        // public void onOpen(WebSocket webSocket, okhttp3.Response response) {
        // System.out.println("Connected to Grafana WebSocket");
        // webSocket.send(tuple.toString());
        // }

        // @Override
        // public void onMessage(WebSocket webSocket, String text) {
        // System.out.println("Received message: " + text);
        // }

        // @Override
        // public void onClosing(WebSocket webSocket, int code, String reason) {
        // System.out.println("Connection closing with code: " + code + " and reason: "
        // + reason);
        // }

        // @Override
        // public void onClosed(WebSocket webSocket, int code, String reason) {
        // System.out.println("Connection closed with code: " + code + " and reason: " +
        // reason);
        // }

        // @Override
        // public void onFailure(WebSocket webSocket, Throwable t, okhttp3.Response
        // response) {
        // System.out.println("Error: " + t.getMessage());
        // }
        // });
        // return tuple;
        // });
        // tasa.print();

        env.execute("information_system_ece_ntua_2022_2023");
    }

    private static void writeToOpenTSDB(Tuple2<Long, Double> answerRow, String sensor, Boolean nextDayFlag) {
        RestClient client = new RestClient();
        String protocol = "http";
        String host = "localhost";
        String port = "4242";
        String path = "/api/put";
        byte[] eventBody = null;
        Double temperature = answerRow.f1;
        String sensorName = sensor;
        Long time = answerRow.f0;
        if (nextDayFlag) {
            time = time + 60 * 60 * 24 * 1000;
        }

        String msg = "{\"metric\": \"%s\", \"timestamp\": %s, \"value\": %s, \"tags\": {\"%s\" : \"%s\" }}";
        DateFormat dF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        String jsonEvent = String.format(msg, sensorName, time, temperature, sensorName, sensorName);
        eventBody = jsonEvent.getBytes();
        String convertedMsg = new String(eventBody, StandardCharsets.UTF_8);

        if (eventBody != null && eventBody.length > 0) {
            HttpResponse res = client.publishToOpenTSDB(protocol, host, port, path,
                    convertedMsg);
            System.out.println("Response: " + res.getStatusLine());
        }
    }
}
