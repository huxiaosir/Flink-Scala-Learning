package org.joisen.java.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author Joisen
 * @Date 2022/12/12 17:18
 * @Version 1.0
 */
public class CommonApiTest {
    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);

        // 1 定义环境配置来创建表执行环境  上下两种方式创建表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
               // .inBatchMode() // 使用批处理模式
                .useBlinkPlanner()
                //.useOldPlanner().build(); //基于老版本planner进行批处理
                .build();
        TableEnvironment tblEnv1 = TableEnvironment.create(settings);

        // 2 创建表
        String createDDL = "create table clickTable (" +
                " username string," +
                " url string,"+
                " ts bigint" +
                " ) with( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.txt', " +
                " 'format' = 'csv' )";
        tblEnv1.executeSql(createDDL);
        // 表对象和注册表转换  调用tableApi进行表的查询转换
        Table clickTable = tblEnv1.from("clickTable");
        Table resTable = clickTable.where($("username").isEqual("Bob")).select($("username"), $("url"));
        tblEnv1.createTemporaryView("result2", resTable);

        // 执行sql进行表的查询转换
        Table resTable1 = tblEnv1.sqlQuery("select username, url from result2");

        // 执行聚合计算的查询转换
        Table aggResult = tblEnv1.sqlQuery("select username,count(url) as cnt from clickTable group by username");


        // 创建一张用于输出的表
        String createOutDDL = "create table outTable (" +
                " username string," +
                " url string"+
                " ) with( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'output', " +
                " 'format' = 'csv' )";
        tblEnv1.executeSql(createOutDDL);

        // 创建一张用于输出到控制台的表
        String createPrintOutDDL = "create table printOutTable (" +
                " username string," +
                " cnt bigint " +
                " ) with( " +
                " 'connector' = 'print' )";
        tblEnv1.executeSql(createPrintOutDDL);

        // 输出表
//        resTable.executeInsert("outTable");
//        resTable.executeInsert("printOutTable");
        aggResult.executeInsert("printOutTable");
    }

}
