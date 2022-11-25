package org.joisen.chapter08

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/23 16:28
 * @Version 1.0
 */
object ConnectTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream1 = env.fromElements(1, 2, 3)
    val stream2 = env.fromElements(1L, 2L, 3L)
    val connectedStreams = stream1.connect(stream2)
    val result = connectedStreams
      .map(new CoMapFunction[Int, Long, String] {
        // 处理来自第一条流的事件
        override def map1(in1: Int): String = "Int: " + in1

        // 处理来自第二条流的事件
        override def map2(in2: Long): String = "Long: " + in2
      })
    result.print()
    env.execute()

  }

}
