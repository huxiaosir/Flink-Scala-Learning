package org.joisen.chapter05

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/20 17:37
 * @Version 1.0
 */
object TransformRichFuncTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Bob", "./cart", 3000L),
      Event("Alice", "./cart", 3000L),
      Event("Mary", "./prod?id=2", 4000L),
      Event("Mary", "./prod?id=3", 6000L),
      Event("Mary", "./prod?id=4", 5000L))

    // 自定义一个MapRichFunction，测试富函数功能
    stream.map(new MyRichMap).print()



    env.execute()
  }
  // 实现自定义的 RichMapFunction
  class MyRichMap() extends RichMapFunction[Event, Long]{
    // 每个并行子任务都只调用一次open和close方法
    override def open(parameters: Configuration): Unit = {
      println("索引号为：" + getRuntimeContext.getIndexOfThisSubtask + " 的任务开始 ")
    }
    override def map(in: Event): Long = in.timestamp

    override def close(): Unit = {
      println("索引号为：" + getRuntimeContext.getIndexOfThisSubtask + " 的任务结束 ")
    }

  }

}
