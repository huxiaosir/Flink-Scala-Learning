package org.joisen.java.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joisen.java.chapter05.ClickSource;
import org.joisen.java.chapter05.Event;

import java.time.Duration;

/**
 * @Author Joisen
 * @Date 2022/12/9 10:51
 * @Version 1.0
 */
public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        stream.keyBy(data -> data.user)
                        .flatMap(new MyFlatMap())
                                .print();

        env.execute();
    }
    // 实现自定义的flatMapFunction 用于keyed State 测试
    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {
        // 定义状态
        ValueState<Event> myValueState;
        ListState<Event> myListState;
        MapState<String, Long> myMapState;
        ReducingState<Event> myReducingState;
        AggregatingState<Event, String> myAggregatingState;
        @Override
        public void open(Configuration parameters) throws Exception {
            myValueState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("my-state", Event.class));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("my-list", Event.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map", String.class, Long.class));
            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("my-reducing",
                    new ReduceFunction<Event>() {
                        @Override
                        public Event reduce(Event event, Event t1) throws Exception {
                            return new Event(event.user, event.url, t1.timestamp);
                        }
                    }
                    , Event.class));
            myAggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("my-agg",
                    new AggregateFunction<Event, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Event event, Long aLong) {
                            return aLong + 1;
                        }

                        @Override
                        public String getResult(Long aLong) {
                            return "account" + aLong;
                        }

                        @Override
                        public Long merge(Long aLong, Long acc1) {
                            return null;
                        }
                    }
                    , Long.class));

        }

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            // 访问和更新状态
//            System.out.println(myValueState.value());
            myValueState.update(event);
            System.out.println("my value: " + myValueState.value());

            myListState.add(event);
            System.out.println("my list: " + myListState.get());
            myMapState.put(event.user, myMapState.get(event.user) == null ? 0 : myMapState.get(event.user)+1);
            System.out.println("mt map value: "+ event.user + " " + myMapState.get(event.user));

            myAggregatingState.add(event);
            System.out.println("my agg: " + myAggregatingState.get());

            myReducingState.add(event);
            System.out.println("my reducing: " + myReducingState.get());
            System.out.println("-------------------------\n");
        }
    }

}
