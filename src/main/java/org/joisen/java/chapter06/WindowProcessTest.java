package org.joisen.java.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
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
public class WindowProcessTest {
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
        // 使用processWindowFunction计算uv
        stream.keyBy(data -> true)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                                .process(new UvCountBuWindow()).print();
        stream.print("data ");

        env.execute();
    }
    // 实现自定义的ProcessWindowFunction， 输出一条统计信息
    public static class UvCountBuWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, java.lang.Iterable<Event> iterable, Collector<String> collector) throws Exception {
            // 用一个hashset保存user
            HashSet<String> userSet = new HashSet<>();
            // 从iterable中遍历数据，放到set中去重
            for (Event event : iterable) {
                userSet.add(event.user);
            }
            int uv = userSet.size();
            // 结合窗口信息
            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect("窗口："+new Timestamp(start) + " ~ " + new Timestamp(end)
                + " UV值为：" + uv
            );
        }
    }
}
