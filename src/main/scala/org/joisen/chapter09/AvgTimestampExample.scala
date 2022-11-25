package org.joisen.chapter09

import org.apache.flink.api.common.functions.{AggregateFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.joisen.chapter05.{ClickSource, Event}

/**
 * @Author Joisen
 * @Date 2022/11/24 10:45
 * @Version 1.0
 */
// 统计5秒内时间戳的平均值
object AvgTimestampExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user)
      .flatMap(new AvgTimestamp)
      .print()

    env.execute()

  }
  // 自定义的RichFlatMapFunction
  class AvgTimestamp extends RichFlatMapFunction[Event, String] {

    // 定义聚合状态
    lazy val avgTsAggState: AggregatingState[Event, Long] = getRuntimeContext.getAggregatingState(new AggregatingStateDescriptor[Event, (Long, Long), Long](
      "avg-ts",
      new AggregateFunction[Event, (Long, Long), Long]{
        override def createAccumulator(): (Long, Long) = (0L, 0L)

        override def add(in: Event, acc: (Long, Long)): (Long, Long) = (acc._1 + in.timestamp, acc._2 + 1)

        override def getResult(acc: (Long, Long)): Long = acc._1 / acc._2

        override def merge(acc: (Long, Long), acc1: (Long, Long)): (Long, Long) = ???
      },
      classOf[(Long, Long)]
    ))
    // 定义一个值状态， 保存当前已经达到的数据个数
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))

    override def flatMap(in: Event, collector: Collector[String]): Unit = {
      avgTsAggState.add(in)
      // 更新count值
      val count: Long = countState.value()
      countState.update(count + 1)
      // 判断是否达到了计数窗口的长度
      if (countState.value() == 5){
        collector.collect(s"${in.user}的平均时间戳为：${avgTsAggState.get()}")
        // 窗口销毁
        countState.clear()
      }

    }

  }

}
