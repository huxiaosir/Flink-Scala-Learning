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
 * @Date 2022/12/13 16:52
 * @Version 1.0
 */
public class TimeAndWindowTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1 在创建表的DDL中直接定义时间属性
        String createDDL = "create table clickTable (" +
                " username string," +
                " url string,"+
                " ts bigint, " +
                " et as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                " WATERMARK for et as et - INTERVAL '1' second" +
                " ) with( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.txt', " +
                " 'format' = 'csv' )";
        tableEnv.executeSql(createDDL);
        // 2 在流转换成Table的时候定义时间属性
        SingleOutputStreamOperator<Event> clickStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        Table clickTable = tableEnv.fromDataStream(clickStream, $("user"), $("url"), $("timestamp").as("ts"),
                $("et").rowtime());

        // 集合查询转换
        // 1 分组聚合
        Table aggTable = tableEnv.sqlQuery("select username, count(1) from clickTable group by username");

        // 2 分组窗口聚合（老版本）
        Table groupWindowResTBL = tableEnv.sqlQuery("select " +
                "username, count(1) as cnt," +
                " TUMBLE_END(et, INTERVAL '10' SECOND) AS endT" +
                " from clickTable " +
                "group by " +
                "username, " +
                "TUMBLE(et, INTERVAL '10' SECOND)");

        // 3 窗口聚合
        // 3.1 滚动窗口
        Table tumbleWindowResTBL = tableEnv.sqlQuery("select username, COUNT(1) as cnt," +
                " window_end as endT " +
                "from table(" +
                "TUMBLE(TABLE clickTable, descriptor(et), interval '10' second))" +
                " group by username, window_end, window_start");
        // 3.2 滑动窗口
        Table hopWindowResTBL = tableEnv.sqlQuery("select username, COUNT(1) as cnt," +
                " window_end as endT " +
                "from table(" +
                "HOP(TABLE clickTable, descriptor(et), interval '5' second, interval '10' second))" +
                " group by username, window_end, window_start");
        // 3.3 累积窗口
        Table cumulateWindowResTBL = tableEnv.sqlQuery("select username, COUNT(1) as cnt," +
                " window_end as endT " +
                "from table(" +
                "CUMULATE(TABLE clickTable, descriptor(et), interval '5' second, interval '10' second))" +
                " group by username, window_end, window_start");
        // 开窗聚合（over）
        Table overWindowResTBL = tableEnv.sqlQuery("select username, " +
                " avg(ts) over(" +
                " partition by username" +
                " order by et" +
                " rows between 3 preceding and current row" +
                ") as avg_ts" +
                " from clickTable");


//        clickTable.printSchema();

//        tableEnv.toChangelogStream(aggTable).print("aggTable");
//        tableEnv.toChangelogStream(groupWindowResTBL).print("groupWindowResTBL");
//        tableEnv.toChangelogStream(tumbleWindowResTBL).print("tumbleWindowResTBL");
//        tableEnv.toChangelogStream(hopWindowResTBL).print("hopWindowResTBL");
//        tableEnv.toChangelogStream(cumulateWindowResTBL).print("cumulateWindowResTBL");
        tableEnv.toChangelogStream(overWindowResTBL).print("overWindowResTBL");

        env.execute();

    }
}
