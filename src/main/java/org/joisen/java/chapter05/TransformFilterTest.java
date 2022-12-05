package org.joisen.java.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Joisen
 * @Date 2022/12/5 20:48
 * @Version 1.0
 */
public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Alice", "./home", 1000L),
                new Event("Mary", "./me", 2000L),
                new Event("Bob", "./cart", 3000L),
                new Event("Cary", "./prod?id=1", 4000L));

//        stream.filter(new MyFilter()).print();
        // 传入匿名类实现
        stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.user.equals("Bob");
            }
        }).print();
        // λ表达式实现
        stream.filter(data -> data.user.equals("Alice")).print("λ ");



        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            return event.user.equals("Mary");
        }
    }

}
