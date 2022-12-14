package org.joisen.java.chapter11;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

/**
 * @Author Joisen
 * @Date 2022/12/14 17:10
 * @Version 1.0
 */
public class UdfTest_TableFunction {
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

        // 2 注册自定义表函数
        tableEnv.createTemporarySystemFunction("MySplit", MySplit.class);

        // 3 调用UDF进行查询转换
        Table resTable = tableEnv.sqlQuery("select usr, url, word, length " +
                "from clickTable, lateral table( MySplit(url)) as T(word, length)" );

        // 4 转换成流进行输出
        tableEnv.toDataStream(resTable).print();
        env.execute();
    }
    // 自定义表函数
    public static class MySplit extends TableFunction<Tuple2<String, Integer>>{
        public void eval(String str){
            String[] fields = str.split("\\?");
            for (String field : fields) {
                collect(Tuple2.of(field, field.length()));
            }
        }
    }
}
