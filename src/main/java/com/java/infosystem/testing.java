package com.java.infosystem;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

public final class testing {

        // *************************************************************************
        // PROGRAM
        // *************************************************************************

        public static void main(String[] args) throws Exception {

                // set up the Java DataStream API
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                // set up the Java Table API
                final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

                final DataStream<Order> orderA = env.fromCollection(
                                Arrays.asList(
                                                new Order(1, 2, 3),
                                                new Order(1, 2, 4),
                                                new Order(4, 5, 2),
                                                new Order(2, 5, 3),
                                                new Order(7, 7, 3),
                                                new Order(1, 1, 1)));

                final DataStream<Order> orderB = env.fromCollection(
                                Arrays.asList(
                                                new Order(2, 5, 3),
                                                new Order(7, 7, 3),
                                                new Order(1, 1, 1)));

                // convert the first DataStream to a Table object
                // it will be used "inline" and is not registered in a catalog
                final Table tableA = tableEnv.fromDataStream(orderA);

                // convert the second DataStream and register it as a view
                // it will be accessible under a name
                tableEnv.createTemporaryView("TableB", orderB);

                // union the two tables
                final Table result = tableEnv.sqlQuery(
                                "SELECT * FROM "
                                                + tableA
                                                + " WHERE c > 2 UNION ALL "
                                                + "SELECT * FROM TableB WHERE c = 3");

                // convert the Table back to an insert-only DataStream of type `Order`
                tableEnv.toAppendStream(result, Order.class).print();

                // after the table program is converted to a DataStream program,
                // we must use `env.execute()` to submit the job
                env.execute();
        }

        // *************************************************************************
        // a DATA TYPES
        // *************************************************************************

        /** Simple POJO. */
        public static class Order {
                public int a;
                public int b;
                public int c;

                // for POJO detection in DataStream API
                public Order() {
                }

                // for structured type detection in Table API
                public Order(int a, int b, int c) {
                        this.a = a;
                        this.b = b;
                        this.c = c;
                }

                @Override
                public String toString() {
                        return "Order{"
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
