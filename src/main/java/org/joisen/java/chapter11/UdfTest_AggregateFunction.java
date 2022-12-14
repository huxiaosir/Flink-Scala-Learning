package org.joisen.java.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * @Author Joisen
 * @Date 2022/12/14 17:23
 * @Version 1.0
 */
public class UdfTest_AggregateFunction {
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
        tableEnv.createTemporarySystemFunction("WeightedAverage", WeightedAverage.class);

        // 3 调用UDF进行查询转换
        Table resTable = tableEnv.sqlQuery("select usr, WeightedAverage(ts, 1) as w_avg " +
                "from clickTable group by usr" );

        // 4 转换成流进行输出
        tableEnv.toChangelogStream(resTable).print();
        env.execute();
    }
    // 单独定义一个累加器类型
    public static class WeightedAvgAccumulator{
        public long sum = 0;
        public int cnt = 0;

    }
    // 实现自定义的聚合函数, 计算加权平均值
    public static class WeightedAverage extends AggregateFunction<Long, WeightedAvgAccumulator>{

        @Override
        public Long getValue(WeightedAvgAccumulator accumulator) {
            if(accumulator.cnt == 0) return null;
            else
                return accumulator.sum / accumulator.cnt;
        }

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        // 累加器计算方法（方法必须是这样定义）
        public void accumulate(WeightedAvgAccumulator accumulator, Long iValue, Integer iWeight){
            accumulator.sum += iValue * iWeight;
            accumulator.cnt += iWeight;
        }
    }

}
