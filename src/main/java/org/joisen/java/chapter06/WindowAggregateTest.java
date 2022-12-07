package org.joisen.java.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joisen.java.chapter05.ClickSource;
import org.joisen.java.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2022/12/7 11:45
 * @Version 1.0
 */
public class WindowAggregateTest {
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
        stream.keyBy(data -> data.user)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                                .aggregate(new AggregateFunction<Event, Tuple2<Long, Integer>, String>() {
                                    @Override
                                    public Tuple2<Long, Integer> createAccumulator() {
                                        return Tuple2.of(0L, 0);
                                    }

                                    @Override
                                    public Tuple2<Long, Integer> add(Event event, Tuple2<Long, Integer> longIntegerTuple2) {
                                        return Tuple2.of(longIntegerTuple2.f0 + event.timestamp, longIntegerTuple2.f1+1);
                                    }

                                    @Override
                                    public String getResult(Tuple2<Long, Integer> longIntegerTuple2) {
                                        Timestamp timestamp = new Timestamp(longIntegerTuple2.f0 / longIntegerTuple2.f1);
                                        return timestamp.toString();
                                    }

                                    @Override
                                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> longIntegerTuple2, Tuple2<Long, Integer> acc1) {
                                        return Tuple2.of(longIntegerTuple2.f0 + acc1.f0, longIntegerTuple2.f1 + acc1.f1);
                                    }
                                })
                                        .print();



        env.execute();
    }

}
