package org.joisen.java.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joisen.java.chapter05.ClickSource;
import org.joisen.java.chapter05.Event;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2022/12/6 21:40
 * @Version 1.0
 */
public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStream<Event> stream = env.fromElements(new Event("Alice", "./home", 1000L),
//                        new Event("Mary", "./me", 2000L),
//                        new Event("Alice", "./cart", 2300L),
//                        new Event("Mary", "./home", 2600L),
//                        new Event("Bob", "./cart", 3000L),
//                        new Event("Bob", "./prod?id=1", 4000L),
//                        new Event("Bob", "./prod?id=2", 5000L),
//                        new Event("Bob", "./home", 6000L),
//                        new Event("Cary", "./prod?id=1", 8000L))

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                // 有序流的watermark生成
//                        .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
//                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                                    @Override
//                                    public long extractTimestamp(Event event, long l) {
//                                        return event.timestamp;
//                                    }
//                                }));
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1L);
                    }
                })
                .keyBy(data -> data.f0)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 滚动事件窗口
//                        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))// 滑动事件时间窗口
//                        .window(EventTimeSessionWindows.withGap(Time.seconds(2))) // 事件时间会话窗口
//                        .countWindow(10, 2) // 滑动计数窗口
        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                return Tuple2.of(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1);
            }
        })
                .print();


        env.execute();
    }
}
