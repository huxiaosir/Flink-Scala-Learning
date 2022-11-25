package org.joisen.chapter07

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.joisen.chapter05.{ClickSource, Event}

/**
 * @Author Joisen
 * @Date 2022/11/23 10:49
 * @Version 1.0
 */
object EventTimeTimerTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.addSource(new CustomSource)
      .assignAscendingTimestamps(_.timestamp)

    stream.keyBy(data => true)
      .process(new KeyedProcessFunction[Boolean, Event, String] {
        override def processElement(i: Event, context: KeyedProcessFunction[Boolean, Event, String]#Context, collector: Collector[String]): Unit = {
          val curTime: Long = context.timerService().currentWatermark()
          collector.collect(s"数据到达， 当前时间为：$curTime, 当前数据时间戳是：${i.timestamp}")
          // 注册一个5秒之后的定时器
          context.timerService().registerEventTimeTimer(curTime + 5 * 1000)

        }

        // 定义定时器触发时的执行逻辑
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Boolean, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
          out.collect("定时器触发，触发时间为："+timestamp)
        }
      })
      .print()

    env.execute()
  }

  class CustomSource extends SourceFunction[Event]{
    override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
      // 直接发出测试数据
      ctx.collect(Event("Mary", "./home", 1000L))
      // 间隔5秒
      Thread.sleep(5000)
      // 继续发出数据
      ctx.collect(Event("Mary", "./home", 2000L))
      Thread.sleep(5000)
      ctx.collect(Event("Mary", "./home", 6000L))
      Thread.sleep(5000)
    }

    override def cancel(): Unit = ???
  }

}
