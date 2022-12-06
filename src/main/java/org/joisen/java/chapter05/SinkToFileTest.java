package org.joisen.java.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @Author Joisen
 * @Date 2022/12/6 15:44
 * @Version 1.0
 */
public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Event> stream = env.fromElements(new Event("Alice", "./home", 1000L),
                new Event("Mary", "./me", 2000L),
                new Event("Alice", "./cart", 2300L),
                new Event("Mary", "./home", 2600L),
                new Event("Bob", "./prod?id=1", 4000L),
                new Event("Bob", "./prod?id=2", 5000L),
                new Event("Bob", "./home", 6000L),
                new Event("Cary", "./prod?id=1", 8000L));

        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().withMaxPartSize(1024 * 1024 * 1024)
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        .build())
                .build();
        stream.map(data -> data.toString()).addSink(streamingFileSink);

        env.execute();
    }
}
