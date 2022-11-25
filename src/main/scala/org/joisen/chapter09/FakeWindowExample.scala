package org.joisen.chapter09

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
object FakeWindowExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user)
      .process(new FakeWindow(10))
      .print()

    env.execute()

  }
  // 自定义的KeyedProcessFunction
  class FakeWindow(size: Long) extends KeyedProcessFunction[String, Event, String] {

    // 定义一个映射状态(key：窗口起始时间， value: 要保存的值)， 用来保存每个窗口的pv值
    lazy val windowPvMapState: MapState[Long, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[Long, Long]("window-pv", classOf[Long], classOf[Long]))

    override def processElement(i: Event, context: KeyedProcessFunction[String, Event, String]#Context, collector: Collector[String]): Unit = {
      // 计算当前数据落入的窗口起始时间戳
      val start: Long = i.timestamp / size * size
      val end: Long = start + size
      // 注册一个定时器，用来触发窗口计算
      context.timerService().registerEventTimeTimer(end - 1)
      // 更新状态，count+1
      if(windowPvMapState.contains(start)){
        val count: Long = windowPvMapState.get(start)
        windowPvMapState.put(start, count+1)
      }else{
        windowPvMapState.put(start, 1L)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 定时器触发时，窗口输出结果
      val start: Long = timestamp + 1 - size
      val pv: Long = windowPvMapState.get(start)
      out.collect(s"url: ${ctx.getCurrentKey} 浏览量为：${pv}  窗口为：${start} ~ ${start + size}")
      // 窗口销毁
      windowPvMapState.remove(start)

    }
  }

}
