package org.joisen.chapter05

import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/20 19:32
 * @Version 1.0
 */
object PartitionReblanceTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.addSource(new ClickSource)

    // 轮询重分区后打印输出
    stream.rebalance.print().setParallelism(4)

    env.execute()

  }

}
