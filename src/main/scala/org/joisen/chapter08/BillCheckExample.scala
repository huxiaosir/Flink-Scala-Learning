package org.joisen.chapter08

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author Joisen
 * @Date 2022/11/23 16:35
 * @Version 1.0
 */
object BillCheckExample {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 来自 app 的支付日志
    val appStream: DataStream[(String, String, Long)] = env
      .fromElements(
        ("order-1", "success", 1000L),
        ("order-2", "success", 2000L)
      )
      .assignAscendingTimestamps(_._3)
    // 来自第三方支付平台的支付日志
    val thirdPartyStream: DataStream[(String, String, String, Long)] = env
      .fromElements(
        ("order-1", "third-party", "success", 3000L),
        ("order-3", "third-party", "success", 4000L)
      )
      .assignAscendingTimestamps(_._4)

    // 连接两条流进行匹配数据检测
    appStream.connect(thirdPartyStream)
      .keyBy(_._1, _._1)
      //          [流1输入类型，流2输入类型，结果类型]
      .process(new CoProcessFunction[(String, String, Long), (String, String, String, Long), String] {
        // 定义状态变量，用来保存已经到达的事件
        var appEvent: ValueState[(String, String, Long)] = _
        var thirdPartyEvent: ValueState[(String, String, String, Long)] = _

        override def open(parameters: Configuration): Unit = {
          appEvent = getRuntimeContext.getState(new ValueStateDescriptor[(String, String, Long)]("app-event", classOf[(String, String, Long)]))
          thirdPartyEvent = getRuntimeContext.getState(new ValueStateDescriptor[(String, String, String, Long)]("thirdParty-event", classOf[(String, String, String, Long)]))
        }

        override def processElement1(in1: (String, String, Long), context: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#Context, collector: Collector[String]): Unit = {
          if(thirdPartyEvent.value() != null){
            collector.collect(s"${in1._1} 对账成功")
            // 清空状态
            thirdPartyEvent.clear()
          }else{
            // 如果另一条流中的事件没有到达，就注册定时器，开始等待
            context.timerService().registerEventTimeTimer(in1._3 + 5000)
            // 保存当前事件到状态中
            appEvent.update(in1)
          }
        }

        override def processElement2(in2: (String, String, String, Long), context: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#Context, collector: Collector[String]): Unit = {
          if (appEvent.value() != null) {
            collector.collect(s"${in2._1} 对账成功")
            // 清空状态
            appEvent.clear()
          } else {
            // 如果另一条流中的事件没有到达，就注册定时器，开始等待
            context.timerService().registerEventTimeTimer(in2._4 + 5000)
            // 保存当前事件到状态中
            thirdPartyEvent.update(in2)
          }
        }

        override def onTimer(timestamp: Long, ctx: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
          // 判断状态是否为空，如果不为空，说明另一条流中对应的事件没来
          if (appEvent.value() != null){
            out.collect(s"${appEvent.value()._1} 对账失败，订单的第三方支付信息未到")
          }
          if (thirdPartyEvent.value() != null) {
            out.collect(s"${thirdPartyEvent.value()._1} 对账失败，app支付信息未到")
          }
          appEvent.clear()
          thirdPartyEvent.clear()
        }

      }).print()

    env.execute()


  }
}
