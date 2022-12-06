package org.joisen.java.chapter05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Joisen
 * @Date 2022/12/6 14:24
 * @Version 1.0
 */
public class TransformRichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(new Event("Alice", "./home", 1000L),
                new Event("Mary", "./me", 2000L),
                new Event("Bob", "./cart", 3000L),
                new Event("Cary", "./prod?id=1", 4000L));

        stream.map(new MyRichMapper()).setParallelism(2).print();
        env.execute();
    }
    // 实现自定义富函数类
    public static class MyRichMapper extends RichMapFunction<Event, Integer>{
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open 生命周期被调用"+ getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public Integer map(Event event) throws Exception {
            return event.url.length();
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }

}
