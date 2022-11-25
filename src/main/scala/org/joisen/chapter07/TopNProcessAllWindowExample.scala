package org.joisen.chapter07

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.joisen.chapter05.{ClickSource, Event}
import org.joisen.chapter07.EventTimeTimerTest.CustomSource

import scala.collection.mutable

/**
 * @Author Joisen
 * @Date 2022/11/23 11:18
 * @Version 1.0
 */
object TopNProcessAllWindowExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)

    // 直接开窗统计
    stream.map(_.url)
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      .process(new ProcessAllWindowFunction[String, String, TimeWindow] {
        override def process(ctx: Context, elements: Iterable[String], out: Collector[String]): Unit = {
          // 1 统计每个url的访问次数
          // 初始化一个map，以url作为key，以count值作为value
          val urlCountMap: mutable.Map[String, Long] = mutable.Map[String, Long]()
          elements.foreach(
            data => urlCountMap.get(data) match {
              case Some(count) => urlCountMap.put(data, count+1)
              case None => urlCountMap.put(data, 1L)
            }
          )
          // 2 对数据进行排序提取
          val urlCountList: List[(String, Long)] = urlCountMap.toList.sortBy(-_._2).take(2)
          // 3 包装信息打印输出
          val res = new StringBuilder()
          res.append(s"=====窗口${ctx.window.getStart} ~ ${ctx.window.getEnd}=====\n")
          for (i <- urlCountList.indices){
            val tuple: (String, Long) = urlCountList(i)
            res.append(s"浏览量Top${i+1} ")
              .append(s"url: ${tuple._1}")
              .append(s"浏览量是：${tuple._2} \n")
          }
          out.collect(res.toString())
        }
      }).print()
    env.execute()


  }
}
