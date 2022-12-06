package org.joisen.java.chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @Author Joisen
 * @Date 2022/12/6 15:07
 * @Version 1.0
 */
public class TransformPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Alice", "./home", 1000L),
                new Event("Mary", "./me", 2000L),
                new Event("Alice", "./cart", 2300L),
                new Event("Mary", "./home", 2600L),
//                new Event("Bob", "./cart", 3000L),
                new Event("Bob", "./prod?id=1", 4000L),
                new Event("Bob", "./prod?id=2", 5000L),
                new Event("Bob", "./home", 6000L),
                new Event("Cary", "./prod?id=1", 8000L));
//        stream.shuffle().print().setParallelism(4); // 随机分区
//        stream.rebalance().print().setParallelism(4); // 轮询分区
//        stream.print().setParallelism(4); // 底层默认轮询
//        stream.rescale().print().setParallelism(4);

//        env.addSource(new RichParallelSourceFunction<Integer>() {
//            @Override
//            public void run(SourceContext<Integer> sourceContext) throws Exception {
//                for (int i = 1; i <= 8; i++) {
//                    if(i % 2 == getRuntimeContext().getIndexOfThisSubtask())
//                        sourceContext.collect(i);
//                }
//            }
//
//            @Override
//            public void cancel() {
//
//            }
//        }).setParallelism(2).rescale().print().setParallelism(4);


        // 实现自定义分区
        env.fromElements(1,2,3,4,5,6,7,8).partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer integer, int i) {
                return integer % 2;
            }
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer integer) throws Exception {
                return integer;
            }
        }).print().setParallelism(4);

        env.execute();
    }
}
