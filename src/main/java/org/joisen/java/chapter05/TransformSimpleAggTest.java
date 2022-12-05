package org.joisen.java.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author Joisen
 * @Date 2022/12/5 20:48
 * @Version 1.0
 */
public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Alice", "./home", 1000L),
                new Event("Mary", "./me", 2000L),
                new Event("Mary", "./cart", 2300L),
                new Event("Mary", "./home", 2600L),
                new Event("Bob", "./cart", 3000L),
                new Event("Bob", "./prod?id=1", 4000L),
                new Event("Bob", "./prod?id=2", 5000L),
                new Event("Bob", "./home", 6000L),
                new Event("Cary", "./prod?id=1", 8000L));

        // 按键分组之后进行聚合, 提取当前用户最近一次访问数据
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp").print("max: ");
        stream.keyBy(data -> data.user).maxBy("timestamp").print("maxBy");


        env.execute();
    }

}
