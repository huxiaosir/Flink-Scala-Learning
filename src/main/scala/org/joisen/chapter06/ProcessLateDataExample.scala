package org.joisen.chapter06

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.joisen.chapter05.{ClickSource, Event}
import org.joisen.chapter06.UrlViewCountExample.{UrlViewCountAgg, UrlViewCountResult}

import java.time.Duration

/**
 * @Author Joisen
 * @Date 2022/11/22 11:18
 * @Version 1.0
 */
object ProcessLateDataExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val stream: DataStream[Event] = env.socketTextStream("hadoop102", 7777)
      .map(data => {
        val fields: Array[String] = data.split(",")
        Event(fields(0).trim, fields(1).trim, fields(2).trim.toLong)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Event](Duration.ofSeconds(5))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[Event] {
            override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
          }
        ))

    // 定义一个侧输出流的输出标签
    val outputTag: OutputTag[Event] = OutputTag[Event]("late-data")

    // 结合使用增量聚合函数和全窗口函数， 包装统计信息
    val result: DataStream[UrlViewCount] = stream.keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      // 指定窗口允许等待的时间
      .allowedLateness(Time.minutes(1))
      // 将迟到数据输出到侧输出流
      .sideOutputLateData(outputTag)
      .aggregate(new UrlViewCountAgg, new UrlViewCountResult)

    result.print("result")

    stream.print("input")

    // 将测输出流的数据进行打印输出
    result.getSideOutput(outputTag).print("late data")



    env.execute()

  }

}
