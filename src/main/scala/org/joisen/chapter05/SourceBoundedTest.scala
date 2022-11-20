package org.joisen.chapter05

import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/19 20:37
 * @Version 1.0
 */
case class Event(user: String, url: String, timestamp: Long)
object SourceBoundedTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 1 从元素中读取数据 （直接列出所有数据）
    val stream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)

    val stream1: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L))

    // 2 从集合中读取数据
    val clicks: List[Event] = List(Event("Mary", "./home", 1000L), Event("Bob", "./cart", 2000L))
    val stream2: DataStream[Event] = env.fromCollection(clicks)

    // 3 从文件中读取数据
    val stream3: DataStream[String] = env.readTextFile("input/clicks.txt")

    // 打印输出
    stream.print()
    stream1.print()
    stream2.print()
    stream3.print()

    env.execute()

  }
}
