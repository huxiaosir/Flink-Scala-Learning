package org.joisen.chapter05

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/20 19:32
 * @Version 1.0
 */
object PartitionShuffleTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.addSource(new ClickSource)

    // shuffle操作
    stream.shuffle.print().setParallelism(4)

    env.execute()

  }

}
