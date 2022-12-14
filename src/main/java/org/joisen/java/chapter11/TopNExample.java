package org.joisen.java.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author Joisen
 * @Date 2022/12/14 15:31
 * @Version 1.0
 */
public class TopNExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1 在创建表的DDL中直接定义时间属性
        String createDDL = "create table clickTable (" +
                " usr string," +
                " url string,"+
                " ts bigint, " +
                " et as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                " WATERMARK for et as et - INTERVAL '1' second" +
                " ) with( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.txt', " +
                " 'format' = 'csv' )";
        tableEnv.executeSql(createDDL);

        // 普通TOPN， 选取当前所有用户中浏览量最大的2个
        Table topNResTbl = tableEnv.sqlQuery("select usr, cnt, row_num " +
                "from(" +
                " select *, row_number() over(" +
                " order by cnt desc" +
                " ) as row_num " +
                " from (select usr, count(url) as cnt from clickTable group by usr )" +
                ") where row_num <= 2");

//        tableEnv.toChangelogStream(topNResTbl).print("top 2:");

        // 窗口TOP N 统计一段时间内的（前2）活跃用户
        String subQuery = "select usr, count(url) as cnt, window_start, window_end" +
                " from table(" +
                "   tumble(table clickTable, descriptor(et), interval '10' second)" +
                ")" +
                " group by usr, window_start, window_end ";

        Table windowTopNResTbl = tableEnv.sqlQuery("select usr, cnt, row_num, window_end " +
                "from (" +
                "   select *, row_number() over(" +
                "       partition by window_start, window_end " +
                "       order by cnt desc" +
                "   ) as row_num " +
                "   from (" + subQuery + " )" +
                ") where row_num <= 2");
        tableEnv.toDataStream(windowTopNResTbl).print("window_top 2:");


        env.execute();
    }
}
