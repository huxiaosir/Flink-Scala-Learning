package org.joisen.chapter06

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.joisen.chapter05.{ClickSource, Event}

/**
 * @Author Joisen
 * @Date 2022/11/22 11:18
 * @Version 1.0
 */

// 定义统计输出的结果数据结构
case class UrlViewCount(url: String, count: Long, winStart: Long, winEnd: Long)

object UrlViewCountExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.addSource(new ClickSource())
      .assignAscendingTimestamps(_.timestamp)

    // 结合使用增量聚合函数和全窗口函数，包装统计信息
    stream.keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .aggregate(new UrlViewCountAgg, new UrlViewCountResult)
      .print()

    env.execute()

  }

  // 实现增量聚合函数   每来一个数据就 + 1
  class UrlViewCountAgg extends AggregateFunction[Event, Long, Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: Event, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = ???
  }

  // 实现全窗口函数
  class UrlViewCountResult extends ProcessWindowFunction[Long, UrlViewCount,String,TimeWindow] {
    override def process(url: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      // 提取需要的数据
      val count: Long = elements.iterator.next()
      val start: Long = context.window.getStart
      val end: Long = context.window.getEnd

      // 输出数据
      out.collect(UrlViewCount(url, count, start, end))

    }
  }


}
