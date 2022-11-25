package org.joisen.chapter06

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.joisen.chapter05.{ClickSource, Event}

/**
 * @Author Joisen
 * @Date 2022/11/22 11:18
 * @Version 1.0
 */
object AggregateFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.addSource(new ClickSource())
      .assignAscendingTimestamps(_.timestamp)

    // 统计pv(page view) 和 uv(unique view) 输出pv/uv
    stream.keyBy(data => true)
      .window( SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)) )
      .aggregate( new PvUv )
      .print()

    env.execute()

  }

  // 实现自定义聚合函数  用一个二元组(Long, Set)来表示中间聚合的(pv,uv)状态
  class PvUv extends AggregateFunction[Event, (Long, Set[String]), Double] {
    override def createAccumulator(): (Long, Set[String]) = (0L, Set[String]())

    // 每来一条数据, 都会进行add叠加聚合
    override def add(in: Event, acc: (Long, Set[String])): (Long, Set[String]) = (acc._1 + 1, acc._2 + in.user)

    // 返回最终计算结果
    override def getResult(acc: (Long, Set[String])): Double = acc._1.toDouble / acc._2.size

    override def merge(acc: (Long, Set[String]), acc1: (Long, Set[String])): (Long, Set[String]) = ???
  }

}
