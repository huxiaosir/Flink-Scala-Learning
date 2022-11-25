package org.joisen.chapter09

import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author Joisen
 * @Date 2022/11/25 10:46
 * @Version 1.0
 */
// 声明样例类
case class Action(userId: String, action: String)
case class Pattern(action1: String, action2: String)

object BroadcastStateExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 定义数据流，读取用户行为事件
    val actionStream: DataStream[Action] = env.fromElements(
      Action("Alice", "login"),
      Action("Alice", "pay"),
      Action("Bob", "login"),
      Action("Bob", "buy")
    )
    // 定义规则流，读取指定的行为模式
    val patternStream: DataStream[Pattern] = env.fromElements(
      Pattern("login", "pay"),
      Pattern("login", "buy")
    )

    // 定义广播状态的描述器
    val patterns: MapStateDescriptor[Unit, Pattern] = new MapStateDescriptor[Unit, Pattern]("patterns", classOf[Unit], classOf[Pattern])
    val broadcastStream: BroadcastStream[Pattern] = patternStream.broadcast(patterns)

    // 连接两条流，进行处理
    actionStream.keyBy(_.userId)
      .connect(broadcastStream)
      .process(new PatternEvaluation)
      .print()

    env.execute()
  }
  // 实现自定义的KeyedBroadcastProcessFunction
  class PatternEvaluation extends KeyedBroadcastProcessFunction[String, Action, Pattern, (String, Pattern)] {
    // 定义值状态，保存上一次用户行为
    lazy val prevActionState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("prev-action", classOf[String]))

    override def processElement(in1: Action, readOnlyContext: KeyedBroadcastProcessFunction[String, Action, Pattern, (String, Pattern)]#ReadOnlyContext, collector: Collector[(String, Pattern)]): Unit = {
      // 从广播状态中获取行为模板
      val pattern: Pattern = readOnlyContext.getBroadcastState(new MapStateDescriptor[Unit, Pattern]("patterns", classOf[Unit], classOf[Pattern]))
        .get(Unit)
      // 从值状态中获取上次行为
      val prevAction: String = prevActionState.value()
      if (pattern != null && prevAction != null) {
        if (pattern.action1 == prevAction && pattern.action2 == in1.action) {
          collector.collect((readOnlyContext.getCurrentKey, pattern))
        }
      }

      // 保存状态
      prevActionState.update(in1.action)
    }
    // 更改广播状态中的数据
    override def processBroadcastElement(in2: Pattern, context: KeyedBroadcastProcessFunction[String, Action, Pattern, (String, Pattern)]#Context, collector: Collector[(String, Pattern)]): Unit = {
      val bcState: BroadcastState[Unit, Pattern] = context.getBroadcastState(new MapStateDescriptor[Unit, Pattern]("patterns", classOf[Unit], classOf[Pattern]))
      bcState.put(Unit, in2)
    }
  }


}
