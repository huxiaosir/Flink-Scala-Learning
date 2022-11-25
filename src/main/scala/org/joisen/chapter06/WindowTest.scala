package org.joisen.chapter06

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.joisen.chapter05.{ClickSource, Event}

/**
 * @Author Joisen
 * @Date 2022/11/22 10:45
 * @Version 1.0
 */
object WindowTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.addSource(new ClickSource())
      .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
        })
      )

    stream.map(data => (data.user, 1))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5))) // 基于事件时间的滚动窗口
//      .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8))) // 基于处理时间的滚动窗口
//      .window(SlidingEventTimeWindows.of(Time.days(1), Time.minutes(10)))// 基于事件时间的滑动窗口
//      .window(EventTimeSessionWindows.withGap(Time.seconds(10))) // 基于事件时间的会话窗口
//      .countWindow(10, 2) // 滑动计数窗口
      .reduce( (state, data) => (data._1, state._2+data._2) )
      .print()

    env.execute()
  }
}
