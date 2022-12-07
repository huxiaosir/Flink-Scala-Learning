package org.joisen.java.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joisen.java.chapter05.ClickSource;
import org.joisen.java.chapter05.Event;
import scala.collection.Iterable;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @Author Joisen
 * @Date 2022/12/7 16:13
 * @Version 1.0
 */
public class UvCountExample {
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
        stream.print("data ");
        // 使用aggregateFunction 和 processWindowFunction计算uv
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .aggregate(new UvAgg(), new UvCountResult())
                                .print();

        env.execute();
    }
    // 自定义aggregateFunction 增量聚合计算uv值
    public static class UvAgg implements AggregateFunction<Event, HashSet<String>, Long>{

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event event, HashSet<String> strings) {
            strings.add(event.user);
            return strings;
        }

        @Override
        public Long getResult(HashSet<String> strings) {
            return (long)strings.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }
    // 自定义实现processFunction， 包装窗口信息输出
    public static class UvCountResult extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, java.lang.Iterable<Long> iterable, Collector<String> collector) throws Exception {
            // 结合窗口信息
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long uv = iterable.iterator().next();
            collector.collect("窗口："+new Timestamp(start) + " ~ " + new Timestamp(end)
                    + " UV值为：" + uv
            );
        }
    }
}
