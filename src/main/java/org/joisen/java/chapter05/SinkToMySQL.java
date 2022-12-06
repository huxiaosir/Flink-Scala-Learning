package org.joisen.java.chapter05;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Joisen
 * @Date 2022/12/6 17:36
 * @Version 1.0
 */
public class SinkToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Alice", "./home", 1000L),
                new Event("Mary", "./me", 2000L),
                new Event("Alice", "./cart", 2300L),
                new Event("Mary", "./home", 2600L),
                new Event("Bob", "./cart", 3000L),
                new Event("Bob", "./prod?id=1", 4000L),
                new Event("Bob", "./prod?id=2", 5000L),
                new Event("Bob", "./home", 6000L),
                new Event("Cary", "./prod?id=1", 8000L));

        stream.addSink(JdbcSink.sink(
                "insert into clicks (user, url) values (?, ?)",
                ((statement, event) -> {
                    statement.setString(1, event.user);
                    statement.setString(2, event.url);
                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("999719")
                        .build()
        ));


        env.execute();
    }
}
