package org.joisen.java.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author Joisen
 * @Date 2022/12/5 20:48
 * @Version 1.0
 */
public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Alice", "./home", 1000L),
                new Event("Mary", "./me", 2000L),
                new Event("Bob", "./cart", 3000L),
                new Event("Cary", "./prod?id=1", 4000L));

//        stream.flatMap(new MyFlatMap()).print();

        stream.flatMap( (Event value, Collector<String> out) -> {
            if(value.user.equals("Bob")) out.collect(value.url);
            else if(value.user.equals("Mary")) {
                out.collect(value.user);
                out.collect(value.url);
                out.collect(value.timestamp.toString());
            }
        } ).returns(new TypeHint<String>() {})
                .print("λ ");

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.url);
            collector.collect(event.timestamp.toString());
        }
    }

}
