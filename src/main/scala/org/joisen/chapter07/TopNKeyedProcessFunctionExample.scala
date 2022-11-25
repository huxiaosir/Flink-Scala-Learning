package org.joisen.chapter07

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.joisen.chapter05.{ClickSource, Event}
import org.joisen.chapter06.UrlViewCount
import org.joisen.chapter06.UrlViewCountExample.{UrlViewCountAgg, UrlViewCountResult}

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.collection.mutable

/**
 * @Author Joisen
 * @Date 2022/11/23 11:18
 * @Version 1.0
 */
object TopNKeyedProcessFunctionExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)


    // 1 结合使用增量聚合函数和全窗口函数，统计每个url的访问次数
    val urlCountStream: DataStream[UrlViewCount] = stream.keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .aggregate(new UrlViewCountAgg, new UrlViewCountResult)

    val resStream: DataStream[String] = urlCountStream.keyBy(_.winEnd)
      .process(new TopN(2))
    resStream.print()

    env.execute()
  }

  // 实现自定义KeyedProcessFunction
  class TopN(n: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{
    // 声明列表状态
    var urlViewCountListState: ListState[UrlViewCount] = _

    override def open(parameters: Configuration): Unit = {
       urlViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("list-state", classOf[UrlViewCount]))
    }

    override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
      // 每来一个数据，就直接放入ListState中
      urlViewCountListState.add(i)
      // 注册一个窗口结束时间1ms之后的定时器
      context.timerService().registerEventTimeTimer(i.winEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 先把数据提取出来放到List中
      val urlViewCountList: List[UrlViewCount] = urlViewCountListState.get().toList
      val topNList: List[UrlViewCount] = urlViewCountList.sortBy(-_.count).take(n)
      // 3 包装信息打印输出
      val res = new StringBuilder()
      res.append(s"=====窗口${timestamp - 1 - 10000} ~ ${timestamp - 1}=====\n")
      for (i <- topNList.indices) {
        val urlViewCount: UrlViewCount = topNList(i)
        res.append(s"浏览量Top${i + 1} ")
          .append(s"url: ${urlViewCount.url}")
          .append(s"浏览量是：${urlViewCount.count} \n")
      }
      out.collect(res.toString())
    }

  }

}
