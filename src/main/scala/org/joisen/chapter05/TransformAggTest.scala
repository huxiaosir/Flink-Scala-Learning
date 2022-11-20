package org.joisen.chapter05

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/20 16:32
 * @Version 1.0
 */
object TransformAggTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),Event("Alice", "./cart", 3000L),Event("Mary", "./prod?id=2", 4000L)
      ,Event("Mary", "./prod?id=3", 6000L), Event("Mary", "./prod?id=4", 5000L))

//    stream.print()

    stream.keyBy(new MyKeySelector)
      .maxBy("timestamp") // max和maxBy输出的区别
      .print()
//    stream.keyBy(_.user)
//    stream.keyBy(_.url)

    env.execute()
  }

  class MyKeySelector extends KeySelector[Event, String]{
    override def getKey(in: Event): String = in.user
  }

}
