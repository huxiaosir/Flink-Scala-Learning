package org.joisen.chapter12

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author Joisen
 * @Date 2022/11/29 19:10
 * @Version 1.0
 */
object NFAExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据源
    val loginEventStream: DataStream[LoginEvent] = env.fromElements(
      LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
      LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
      LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
      LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
      LoginEvent("user_2", "192.168.1.29", "success", 6000L),
      LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
      LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
    ).assignAscendingTimestamps(_.timestamp)

    val resStream: DataStream[String] = loginEventStream.keyBy(_.userId)
      .flatMap(new StateMachineMapper())

    resStream.print()

    env.execute()

  }
  // 实现自定义的RichFlatmapFunction
  class StateMachineMapper extends  RichFlatMapFunction[LoginEvent, String] {
    // 定义一个状态机的状态
    lazy val currentState: ValueState[State] = getRuntimeContext.getState(new ValueStateDescriptor[State]("state", classOf[State]))

    override def flatMap(in: LoginEvent, collector: Collector[String]): Unit = {
      if(currentState.value() == null){
        currentState.update(Initial)
      }

      val nextState: State = transition(currentState.value(), in.eventType)
      nextState match {
        case Matched => collector.collect(s"${in.userId}连续三次登录失败！")
        case Terminal => currentState.update(Initial)
        case _ => currentState.update(nextState)
      }
    }
  }

  // 将状态state定义为封闭的特征
  sealed trait State
  case object Initial extends State
  case object Terminal extends State
  case object Matched extends State
  case object S1 extends State
  case object S2 extends State

  // 定义状态转移函数
  def transition(state: State, eventType: String): State ={
    (state, eventType) match {
      case (Initial, "success") => Terminal
      case (Initial, "fail") => S1
      case (S1, "success") => Terminal
      case (S1, "fail") => S2
      case (S2, "success") => Terminal
      case (S2, "fail") => Matched

    }
  }

}
