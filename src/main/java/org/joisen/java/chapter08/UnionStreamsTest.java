package org.joisen.java.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.joisen.java.chapter05.ClickSource;
import org.joisen.java.chapter05.Event;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2022/12/8 15:15
 * @Version 1.0
 */
public class UnionStreamsTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("hadoop102",7777)
                .map(data -> {
                    String[] fields = data.split(",");
                    return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
                );
        stream1.print("stream1 ");
        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("hadoop102",9999)
                .map(data -> {
                    String[] fields = data.split(",");
                    return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
                );
        stream2.print("stream2 ");
        // 合并两条流
        stream1.union(stream2)
                        .process(new ProcessFunction<Event, String>() {
                            @Override
                            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                                out.collect("水位线为：" + ctx.timerService().currentWatermark());
                            }
                        }).print();

        env.execute();
    }
}
