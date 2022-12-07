package org.joisen.java.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joisen.java.chapter05.ClickSource;
import org.joisen.java.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @Author Joisen
 * @Date 2022/12/7 11:45
 * @Version 1.0
 */
public class WindowAggregateTest_PvUv {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        // 所有数据放在一起统计pv和uv
        stream.keyBy(data -> true)
                        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                                .aggregate(new AvgPv()).print();
        stream.print("data ");
        env.execute();
    }
    // 自定义一个aggregate function  用long保存pv个数，用hashset做uv去重
    public static class AvgPv implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double>{

        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            // 每来一条数据，pv个数加1， 将user放入hashset中
            longHashSetTuple2.f1.add(event.user);
            return Tuple2.of(longHashSetTuple2.f0+1, longHashSetTuple2.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            // 窗口触发时，输出pv和uv的比值
            return (double) longHashSetTuple2.f0 / longHashSetTuple2.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }
    }
}
