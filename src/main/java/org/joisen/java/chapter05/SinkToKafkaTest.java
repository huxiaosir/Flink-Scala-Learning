package org.joisen.java.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Author Joisen
 * @Date 2022/12/6 15:53
 * @Version 1.0
 */
public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 从kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        DataStreamSource<String> kfStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        // 2 用flink进行转换处理
        SingleOutputStreamOperator<String> result = kfStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] split = s.split(",");
                return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim())).toString();
            }
        });

        // 结果数据写入kafka
        result.addSink(new FlinkKafkaProducer<String>("hadoop102:9092","events",new SimpleStringSchema()));

        env.execute();
    }
}
