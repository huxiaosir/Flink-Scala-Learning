package org.joisen.chapter05

import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author Joisen
 * @Date 2022/11/20 15:52
 * @Version 1.0
 */
object TransformFlatMapTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),Event("Alice", "./cart", 2000L))

    stream.flatMap(new UserFlatMap).print()

    env.execute()
  }

  class UserFlatMap extends FlatMapFunction[Event, String]{
    override def flatMap(t: Event, collector: Collector[String]): Unit = {
      // 如果当前数据是Mary的点击事件，那么直接输出 user
      if(t.user == "Mary"){
        collector.collect(t.user)
      }
      // 如果当前数据是Bob的点击事件，那么就输出user和url
      else if(t.user == "Bob"){
        collector.collect(t.user)
        collector.collect(t.url)
      }

    }
  }

}
