package org.joisen.java.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.joisen.java.chapter05.Event;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2022/12/6 20:52
 * @Version 1.0
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> stream = env.fromElements(new Event("Alice", "./home", 1000L),
                new Event("Mary", "./me", 2000L),
                new Event("Alice", "./cart", 2300L),
                new Event("Mary", "./home", 2600L),
                new Event("Bob", "./cart", 3000L),
                new Event("Bob", "./prod?id=1", 4000L),
                new Event("Bob", "./prod?id=2", 5000L),
                new Event("Bob", "./home", 6000L),
                new Event("Cary", "./prod?id=1", 8000L))
                // 有序流的watermark生成
//                        .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
//                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                                    @Override
//                                    public long extractTimestamp(Event event, long l) {
//                                        return event.timestamp;
//                                    }
//                                }))
        // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));


        env.execute();
    }
}
