package org.joisen.java.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2022/12/9 11:44
 * @Version 1.0
 */
public class TwoStreamJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple3<String,String,Long>> stream1 = env.fromElements(
                Tuple3.of("a", "stream-1", 1000L),
                Tuple3.of("b", "stream-1", 2000L),
                Tuple3.of("a", "stream-1", 3000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String,String,Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                        return 0;
                    }
                }));

        SingleOutputStreamOperator<Tuple3<String,String,Long>> stream2 = env.fromElements(
                Tuple3.of("a", "stream-2", 4000L),
                Tuple3.of("b", "stream-2", 5000L),
                Tuple3.of("a", "stream-2", 6000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String,String,Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                        return 0;
                    }
                }));

        // 自定义列表状态进行全外联结
        stream1.keyBy(data -> data.f0)
                        .connect(stream2.keyBy(data -> data.f0))
                                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                                    // 定义状态保存两台流中到达的所有数据
                                    ListState<Tuple3<String, String, Long>> stream1ListState;
                                    ListState<Tuple3<String, String, Long>> stream2ListState;

                                    @Override
                                    public void open(Configuration parameters) throws Exception {
                                        stream1ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream1ListState", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG)));
                                        stream2ListState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, String, Long>>("stream2ListState", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG)));
                                    }

                                    @Override
                                    public void processElement1(Tuple3<String, String, Long> left, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                                        // 获取另一条流中的所有数据
                                        for(Tuple3<String, String, Long> right: stream2ListState.get()){
                                            out.collect(left + " => " + right);
                                        }
                                        stream1ListState.add(Tuple3.of(left.f0, left.f1, left.f2));
                                    }

                                    @Override
                                    public void processElement2(Tuple3<String, String, Long> right, CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                                        // 获取另一条流中的所有数据
                                        for(Tuple3<String, String, Long> left: stream1ListState.get()){
                                            out.collect(left + " => " + right);
                                        }
                                        stream1ListState.add(Tuple3.of(right.f0, right.f1, right.f2));
                                    }
                                }).print();

        env.execute();
    }
}
