package org.joisen.chapter07

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.joisen.chapter05.{ClickSource, Event}

/**
 * @Author Joisen
 * @Date 2022/11/22 19:03
 * @Version 1.0
 */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)

    stream.process(new ProcessFunction[Event, String] {
      override def processElement(value: Event, context: ProcessFunction[Event, String]#Context, collector: Collector[String]): Unit = {
        if(value.user.equals("Mary"))
          collector.collect(value.user)
        else if(value.user.equals("Bob")){
          collector.collect(value.user)
          collector.collect(value.url)
        }
        println(getRuntimeContext.getIndexOfThisSubtask)
        println(context.timerService().currentWatermark())

      }
    })
      .print()

    env.execute()
  }
}
