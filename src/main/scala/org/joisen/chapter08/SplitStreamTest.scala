package org.joisen.chapter08

import org.apache.flink.streaming.api.scala._
import org.joisen.chapter05.{ClickSource, Event}

/**
 * @Author Joisen
 * @Date 2022/11/23 15:32
 * @Version 1.0
 */
object SplitStreamTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.addSource(new ClickSource)

    // 按照不同用户的点击时间，进行分流操作
    val maryStream: DataStream[Event] = stream.filter(_.user == "Mary")
    val bobStream: DataStream[Event] = stream.filter(_.user == "Bob")
    val elseStream: DataStream[Event] = stream.filter(data => data.user != "Mary" && data.user != "Bob")

    maryStream.print("Mary ")
    bobStream.print("Bob ")
    elseStream.print("Else ")

    env.execute()

  }
}
