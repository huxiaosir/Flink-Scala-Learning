package org.joisen.java.wc_java;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author Joisen
 * @Date 2022/12/4 15:40
 * @Version 1.0
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2 从文件中读取文件
        DataSource<String> lineData = env.readTextFile("D:\\product\\JAVA\\Project\\FlinkTutorial\\src\\main\\resources\\hello.txt");

        // 3 将每行数据进行分词，转换成二元数组
        // 使用 λ表达式进行数据转换，需要两个参数（输入数据类型，转换后输出类型），
        // flink中flatmap操作的输出需要通过Collector来处理，Collector里面的才是输出数据类型
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineData.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");// 进行分词
            for (String word : words) { // 转换成二元组输出
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4 按照word进行分组  根据索引号作为key进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);

        // 5 组内进行聚合统计  根据索引号进行聚合操作
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        sum.print();


    }
}
