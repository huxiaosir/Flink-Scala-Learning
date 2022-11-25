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
object FullWindowFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.addSource(new ClickSource())
      .assignAscendingTimestamps(_.timestamp)

    // 测试全窗口函数， 统计uv
    stream.keyBy(data => "key")
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new UvCountByWindow)
      .print()

    env.execute()

  }
// 自定义实现ProcessWindowFunction

  class UvCountByWindow extends ProcessWindowFunction[Event, String, String, TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {
      // 使用一个Set进行去重操作
      var userSet: Set[String] = Set[String]()

      // 从elements中提取所有数据， 依次放入set中去重
      elements.foreach(userSet += _.user)
      val uv: Int = userSet.size

      // 提取窗口信息包装String进行输出
      val windowEnd: Long = context.window.getEnd
      val windowStart: Long = context.window.getStart

      out.collect(s"窗口： $windowStart ~ $windowEnd 的uv值为：$uv")

    }
  }

}
