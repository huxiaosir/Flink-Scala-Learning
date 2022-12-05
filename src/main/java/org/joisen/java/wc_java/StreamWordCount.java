package org.joisen.java.wc_java;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author Joisen
 * @Date 2022/12/4 17:33
 * @Version 1.0
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从参数中提取主机名和端口号
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        Integer port = parameterTool.getInt("port");

//        DataStreamSource<String> lineDataStreamSource = env.socketTextStream(host, port);
        DataStreamSource<String> lineDataStreamSource = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> wordAndOneGroup = wordAndOneTuple.keyBy(data -> data.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        sum.print();

        env.execute();


    }
}
