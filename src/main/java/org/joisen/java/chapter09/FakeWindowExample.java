package org.joisen.java.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.joisen.java.chapter05.ClickSource;
import org.joisen.java.chapter05.Event;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2022/12/9 15:21
 * @Version 1.0
 */
public class FakeWindowExample {
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
        stream.print("input ");

        stream.keyBy(data -> data.url)
                        .process(new FakeWindowResult(10000L))
                                .print();

        env.execute();
    }
    // 实现自定义的KeyedProcessFunction
    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String>{
        private Long windowSize; // 窗口大小

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }
        // 定义状态 MapState, 用来保存每个窗口中统计的count值
        MapState<Long, Long> windowUrlCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            windowUrlCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-count", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，根据时间戳判断属于哪个窗口（窗口分配器）
            Long windowStart = value.timestamp / windowSize * windowSize;
            Long windowEnd = windowStart + windowSize;
            // 注册 end-1的定时器
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);
            // 更新状态，进行增量聚合
            if (windowUrlCountMapState.contains(windowStart)) {
                Long count = windowUrlCountMapState.get(windowStart);
                windowUrlCountMapState.put(windowStart, count+1);
            }else {
                windowUrlCountMapState.put(windowStart, 1L);
            }
        }
        // 定时器触发时输出计算结果

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            long windowEnd = timestamp + 1;
            long windowStart = windowEnd - windowSize;
            Long count = windowUrlCountMapState.get(windowStart);
            out.collect("窗口" +new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd) + "   url: " + ctx.getCurrentKey() + "  count: " +count);
            // 模拟窗口关闭， 清除map中对应的key-value
            windowUrlCountMapState.remove(windowStart);
        }
    }

}
