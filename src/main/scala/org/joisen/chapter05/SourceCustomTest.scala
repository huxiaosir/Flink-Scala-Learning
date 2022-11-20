package org.joisen.chapter05

import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/20 15:16
 * @Version 1.0
 */
object SourceCustomTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取自定义的数据源
    val stream: DataStream[Event] = env.addSource(new ClickSource).setParallelism(2)
    stream.print()

    env.execute()

  }
}
