package org.joisen.java.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Author Joisen
 * @Date 2022/12/14 16:57
 * @Version 1.0
 */
public class UdfTest_ScalarFunction {
    public static void main(String[] args) throws Exception {
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

        // 2 注册自定义标量函数
        tableEnv.createTemporarySystemFunction("MyHash", MyHashFunc.class);

        // 3 调用UDF进行查询转换
        Table resTable = tableEnv.sqlQuery("select usr, MyHash(usr) from clickTable");

        // 4 转换成流进行输出
        tableEnv.toDataStream(resTable).print();
        env.execute();
    }
    // 自定义实现 ScalarFunction
    public static class MyHashFunc extends ScalarFunction{
        public int eval(String str){
            return str.hashCode();
        }
    }
}
