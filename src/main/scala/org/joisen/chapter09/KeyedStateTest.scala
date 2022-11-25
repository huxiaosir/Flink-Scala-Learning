package org.joisen.chapter09

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ListState, ListStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.joisen.chapter05.{ClickSource, Event}

/**
 * @Author Joisen
 * @Date 2022/11/24 10:45
 * @Version 1.0
 */
object KeyedStateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user)
      .flatMap(new MyMap)
      .print()

    env.execute()

  }
  // 自定义各异RichMapFunction
  class MyMap extends RichFlatMapFunction[Event,String]{
    // 定义一个值状态
    private var valueState: ValueState[Event] = _
    private var listState: ListState[Event] = _

    private var reducingState:ReducingState[Event] = _
    private var aggState: AggregatingState[Event,String] = _

    override def open(parameters: Configuration): Unit = {
      valueState = getRuntimeContext.getState(new ValueStateDescriptor[Event]("my-value", classOf[Event]))
      listState = getRuntimeContext.getListState(new ListStateDescriptor[Event]("my-list", classOf[Event]))

      reducingState = getRuntimeContext.getReducingState(new ReducingStateDescriptor[Event]("my-reduce",
        new ReduceFunction[Event] {
          override def reduce(t: Event, t1: Event): Event = Event(t.user,t.url,t.timestamp)
        } , classOf[Event]))
      aggState = getRuntimeContext.getAggregatingState(new AggregatingStateDescriptor[Event, Long, String]("my-agg",
        new AggregateFunction[Event, Long, String] {
          override def createAccumulator(): Long = 0L

          override def add(in: Event, acc: Long): Long = acc + 1

          override def getResult(acc: Long): String ="聚合状态为：" + acc.toString

          override def merge(acc: Long, acc1: Long): Long = ???
        }, classOf[Long]
      ))

    }

    override def flatMap(in: Event, collector: Collector[String]): Unit = {
      // 对状态进行操作
      println("值状态为： " + valueState.value())
      valueState.update(in)
      println("值状态为： " + valueState.value())

      listState.add(in)

      println("-=-=-=-=-=-=-=-=")
      reducingState.add(in)
      println(reducingState.get())
      println("-=-=-=-=-=-=-=-=")
      aggState.add(in)
      println(aggState.get())

    }
  }
}
