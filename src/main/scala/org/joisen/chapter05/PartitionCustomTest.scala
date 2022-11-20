package org.joisen.chapter05

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/20 19:32
 * @Version 1.0
 */
object PartitionCustomTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)

    // 自定义重分区策略 操作
    stream.partitionCustom(new Partitioner[Int]{
      override def partition(k: Int, i: Int): Int = {
        k % 2
      }
    }, data=>data)
      .print().setParallelism(4)

    env.execute()

  }

}
