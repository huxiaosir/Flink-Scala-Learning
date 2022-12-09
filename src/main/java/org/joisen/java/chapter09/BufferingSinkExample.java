package org.joisen.java.chapter09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.joisen.java.chapter05.ClickSource;
import org.joisen.java.chapter05.Event;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author Joisen
 * @Date 2022/12/9 19:01
 * @Version 1.0
 */
public class BufferingSinkExample {
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
        // 批量缓存输出
        stream.addSink(new BufferingSink(10));


        env.execute();
    }
    // 自定义实现 SinkFunction
    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        // 定义当前类的属性
        private final int threshold;
        private List<Event> bufferedElements;

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value); // 缓存到列表
            // 判断如果达到阈值，就批量写入
            if(bufferedElements.size() == threshold){
                // 用打印到控制台的方式模拟写入外部系统
                for (Event ele : bufferedElements) {
                    System.out.println(ele);
                }
                System.out.println("===========输出完毕=========");
                bufferedElements.clear();
            }
        }

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }
        // 定义一个算子状态
        private ListState<Event> checkPointedState;
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 清空状态
            checkPointedState.clear();
            // 对状态进行持久化，复制缓存的列表到列表状态
            for (Event ele : bufferedElements) {
                checkPointedState.add(ele);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 定义算子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffered-elements", Event.class);
            checkPointedState = context.getOperatorStateStore().getListState(descriptor);
            // 如果从故障恢复，需要将ListState中的所有元素复制到列表中
            if (context.isRestored()) {
                for (Event event : checkPointedState.get()) {
                    bufferedElements.add(event);
                }
            }
        }
    }

}
