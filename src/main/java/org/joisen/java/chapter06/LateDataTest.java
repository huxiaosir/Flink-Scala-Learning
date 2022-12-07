package org.joisen.java.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.joisen.java.chapter05.ClickSource;
import org.joisen.java.chapter05.Event;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2022/12/7 17:02
 * @Version 1.0
 */
public class LateDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String s) throws Exception {
                        String[] fields = s.split(",");
                        return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                    }
                })
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        stream.print("data ");
        // 定义一个输出标签
        OutputTag<Event> late = new OutputTag<Event>("late");
        // 使用aggregateFunction 和 processWindowFunction计算uv
        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1)) // 设置允许迟到的时间
                .sideOutputLateData(late) // 侧输出流数据
                .aggregate(new UrlCountViewExample.UrlViewCountAgg(), new UrlCountViewExample.UrlViewCountResult());

        result.print("result");
        result.getSideOutput(late).print("late"); // 输出侧输出流


        env.execute();
    }
}
