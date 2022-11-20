package org.joisen.chapter05

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * @Author Joisen
 * @Date 2022/11/20 10:20
 * @Version 1.0
 */
object SourceKafkaTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 用properties保存kafka连接的相关配置
    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id","consumer-group")

    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties))
    stream.print()

    env.execute()

  }
}
