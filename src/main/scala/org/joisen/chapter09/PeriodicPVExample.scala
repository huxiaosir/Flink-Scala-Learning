package org.joisen.chapter09

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.joisen.chapter05.{ClickSource, Event}

/**
 * @Author Joisen
 * @Date 2022/11/24 10:45
 * @Version 1.0
 */
object PeriodicPVExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user)
      .process(new PeriodicPv)
      .print()

    env.execute()

  }
  // 自定义的KeyedProcessFunction
  class PeriodicPv extends KeyedProcessFunction[String, Event, String]{
    // 定义值状态，保存当前用户的pv数据
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
    lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

    override def processElement(i: Event, context: KeyedProcessFunction[String, Event, String]#Context, collector: Collector[String]): Unit = {
      // 每来一条数据，就将状态中的count加1
      val count: Long = countState.value()
      countState.update(count+1)
      // 注册定时器，每隔10秒输出一次统计结果
      if (timerTsState.value() == 0L){
        context.timerService().registerEventTimeTimer(i.timestamp + 10 * 1000L)
        // 更新状态
        timerTsState.update(i.timestamp + 10 * 1000L)
      }
    }
    // 定时器触发，输出当前的统计结果
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect(s"用户${ctx.getCurrentKey}的pv值为： ${countState.value()}")
      // 清理状态
      timerTsState.clear()
    }
  }

}
