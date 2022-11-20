package org.joisen.chapter05

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
 * @Author Joisen
 * @Date 2022/11/20 20:53
 * @Version 1.0
 */
object SinkToKafkaTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 读取文件数据
    val stream: DataStream[String] = env.readTextFile("input/clicks.txt")

    // 将数据写入kafka
    stream.addSink(new FlinkKafkaProducer[String]("hadoop102:9092", "clicks", new SimpleStringSchema()))

    env.execute()


  }
}
