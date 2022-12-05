package org.joisen.java.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @Author Joisen
 * @Date 2022/12/5 19:40
 * @Version 1.0
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文本中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        // 从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(1);
        nums.add(7);
        nums.add(5);
        DataStreamSource<Integer> stream2 = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Mary", "./me", 2000L));
        events.add(new Event("Mary", "./cart", 3000L));
        events.add(new Event("Mary", "./prod?id=1", 4000L));

        DataStreamSource<Event> stream3 = env.fromCollection(events);

        DataStreamSource<String> stream4 = env.socketTextStream("hadoop102", 7777);

        // 从kafka中获取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

//        DataStreamSource<String> kfStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

//        stream1.print();
//        stream2.print();
//        stream3.print();
        stream4.print();
//        kfStream.print();




        env.execute();
    }
}
