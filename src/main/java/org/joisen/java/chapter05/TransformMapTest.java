package org.joisen.java.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Joisen
 * @Date 2022/12/5 20:48
 * @Version 1.0
 */
public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 进行转换计算， 提取user字段
        // 1 使用自定义类，实现MapFunction接口
        DataStreamSource<Event> stream = env.fromElements(new Event("Alice", "./home", 1000L),
                new Event("Mary", "./me", 2000L),
                new Event("Bob", "./cart", 3000L),
                new Event("Cary", "./prod?id=1", 4000L));
        // 2 使用匿名类实现MapFunction接口
        stream.map(new MapFunction<Event, String>() {

            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        }).print();

        // 3 使用lambda表达式
        stream.map( data -> data.user).print("λ");



//        stream.map(new MyMapper()).print();


        env.execute();
    }
    // 自定义MapFunction
    public static class MyMapper implements MapFunction<Event, String>{

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }

}
