package org.joisen.chapter05

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/20 19:32
 * @Version 1.0
 */
object PartitionBroadcastTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取自定义的数据源
    val stream: DataStream[Event] = env.addSource(new ClickSource)

    // 广播
    stream.broadcast.print().setParallelism(4)

    env.execute()

  }

}
