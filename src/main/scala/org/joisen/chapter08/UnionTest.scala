package org.joisen.chapter08

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.joisen.chapter05.Event

/**
 * @Author Joisen
 * @Date 2022/11/23 15:57
 * @Version 1.0
 */
object UnionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取两条流进行合并
    val stream1: DataStream[Event] = env.socketTextStream("hadoop102", 7777)
      .map(data => {
        val fileds: Array[String] = data.split(",")
        Event(fileds(0).trim, fileds(1).trim, fileds(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp)

    val stream2: DataStream[Event] = env.socketTextStream("hadoop102", 8888)
      .map(data => {
        val fileds: Array[String] = data.split(",")
        Event(fileds(0).trim, fileds(1).trim, fileds(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp)

    stream1.union(stream2)
      .process(new ProcessFunction[Event, String] {
        override def processElement(i: Event, context: ProcessFunction[Event, String]#Context, collector: Collector[String]): Unit = {
          collector.collect(s"当前水位线：${context.timerService().currentWatermark()}")
        }
      })
      .print()

    env.execute()
  }
}
