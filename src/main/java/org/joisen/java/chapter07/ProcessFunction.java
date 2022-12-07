package org.joisen.java.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joisen.java.chapter05.ClickSource;
import org.joisen.java.chapter05.Event;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2022/12/7 19:30
 * @Version 1.0
 */
public class ProcessFunction {
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
        stream.process(new org.apache.flink.streaming.api.functions.ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, org.apache.flink.streaming.api.functions.ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                if(event.user.equals("Mary")){
                    collector.collect(event.user + " clicks " + event.url);
                } else if (event.user.equals("Bob")) {
                    collector.collect(event.user);
                    collector.collect(event.user);
                }
                System.out.println("timestamp: " + context.timestamp());
                System.out.println("watermark: " + context.timerService().currentWatermark());

                System.out.println(getRuntimeContext().getIndexOfThisSubtask());
            }

        }).print();

        env.execute();

    }
}
