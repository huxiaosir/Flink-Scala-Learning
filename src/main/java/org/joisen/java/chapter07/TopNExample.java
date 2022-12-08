package org.joisen.java.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.joisen.java.chapter05.ClickSource;
import org.joisen.java.chapter05.Event;
import org.joisen.java.chapter06.UrlCountViewExample;
import org.joisen.java.chapter06.UrlViewCount;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;


/**
 * @Author Joisen
 * @Date 2022/12/8 10:55
 * @Version 1.0
 */
public class TopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
                );
        // 1 按照url分组，统计窗口内每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlCountViewExample.UrlViewCountAgg(), new UrlCountViewExample.UrlViewCountResult());
        urlCountStream.print("url count");

        // 2 对于同一窗口统计出的访问量，进行收集和排序
        urlCountStream.keyBy(data -> data.windowEnd)
                        .process(new TopNProcessResult(2))
                                .print();

        env.execute();
    }
    // 实现自定义的KeyedProcessFunction
    public static class TopNProcessResult extends KeyedProcessFunction<Long, UrlViewCount, String>{
        // 定义一个属性n
        private Integer n;
        // 定义列表状态
        private ListState<UrlViewCount> urlViewCountListState;
        public TopNProcessResult(Integer n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-count-list", Types.POJO(UrlViewCount.class))
            );
        }

        @Override
        public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            // 将数据保存到状态中
            urlViewCountListState.add(value);
            // 注册windowEnd + 1ms的定时器
            ctx.timerService().registerProcessingTimeTimer(ctx.getCurrentKey()+1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCount> list = new ArrayList<>();

            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                list.add(urlViewCount);
            }
            list.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("----------------- \n");
            result.append("窗口结束时间：" + new Timestamp(ctx.getCurrentKey()) + "\n");
            // 取list的前两个，包装信息输出
            for (int i = 0; i <2; i++) {
                UrlViewCount viewCount = list.get(i);
                String info = "No. " + (i+1) + " "
                        + "url: " + viewCount.url + " "
                        + "访问量: "+ viewCount.count+" \n";
                result.append(info);
            }
            result.append("----------------- \n");
            out.collect(result.toString());

        }
    }
}
