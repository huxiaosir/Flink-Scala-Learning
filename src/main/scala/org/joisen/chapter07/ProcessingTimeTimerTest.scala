package org.joisen.chapter07

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.joisen.chapter05.{ClickSource, Event}

/**
 * @Author Joisen
 * @Date 2022/11/23 10:49
 * @Version 1.0
 */
object ProcessingTimeTimerTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)

    stream.keyBy(data => true)
      .process(new KeyedProcessFunction[Boolean, Event, String] {
        override def processElement(i: Event, context: KeyedProcessFunction[Boolean, Event, String]#Context, collector: Collector[String]): Unit = {
          val curTime: Long = context.timerService().currentProcessingTime()
          collector.collect("数据到达， 当前时间为：" + curTime)
          // 注册一个5秒之后的定时器
          context.timerService().registerProcessingTimeTimer(curTime + 5 * 1000)

        }

        // 定义定时器触发时的执行逻辑
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Boolean, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
          out.collect("定时器触发，触发时间为："+timestamp)
        }
      })
      .print()

    env.execute()


  }
}
