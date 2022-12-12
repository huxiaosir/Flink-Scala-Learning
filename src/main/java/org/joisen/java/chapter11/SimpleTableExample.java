package org.joisen.java.chapter11;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.joisen.java.chapter05.ClickSource;
import org.joisen.java.chapter05.Event;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author Joisen
 * @Date 2022/12/12 15:01
 * @Version 1.0
 */
public class SimpleTableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 将DataStream转换成Table
        Table eventTable = tableEnv.fromDataStream(eventStream);
        // 直接写SQL进行转换
        Table resTable1 = tableEnv.sqlQuery("select user, url from " + eventTable);

        // 基于Table直接转换
        Table resTable2 = eventTable.select($("user"), $("url"))
                .where($("user").isEqual("Alice"));

        tableEnv.toDataStream(resTable1).print("result1");
        tableEnv.toDataStream(resTable2).print("result2");
        env.execute();
    }
}
