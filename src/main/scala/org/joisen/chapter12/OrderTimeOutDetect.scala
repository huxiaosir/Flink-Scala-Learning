package org.joisen.chapter12

import org.apache.flink.cep.functions.{PatternProcessFunction, TimedOutPartialMatchHandler}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.util

/**
 * @Author Joisen
 * @Date 2022/11/29 17:16
 * @Version 1.0
 */
// 定义订单事件样例类
case class OrderEvent(userId: String, orderId: String, eventType: String, timestamp: Long)

object OrderTimeOutDetect {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1 读取数据源
    val stream: KeyedStream[OrderEvent, String] = env.fromElements(
      OrderEvent("user_1", "order_1", "create", 1000L),
      OrderEvent("user_2", "order_2", "create", 2000L),
      OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
      OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
      OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
      OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
    ).assignAscendingTimestamps(_.timestamp)
      .keyBy(_.orderId) // 按照订单 ID 分组

    // 2 定义检测的模式
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("create").where(_.eventType.equals("create"))
      .followedBy("pay").where(_.eventType.equals("pay"))
      .within(Time.minutes(15))

    // 3 将模式应用到事件流上
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(stream, pattern)

    // 4 检测匹配事件和部分匹配的超时时间
    val payedOrderStream: DataStream[String] = patternStream.process(new OrderPayDetect())
    payedOrderStream.getSideOutput(new OutputTag[String]("timeout")).print("timeout")
    payedOrderStream.print("payed")

    env.execute()

  }
  class OrderPayDetect extends PatternProcessFunction[OrderEvent, String] with TimedOutPartialMatchHandler[OrderEvent] {
    override def processMatch(map: util.Map[String, util.List[OrderEvent]], context: PatternProcessFunction.Context, collector: Collector[String]): Unit = {
      // 处理正常支付的匹配事件
      val payEvent: OrderEvent = map.get("pay").get(0)
      collector.collect(s"订单${payEvent.orderId}已经支付成功！")
    }

    override def processTimedOutMatch(map: util.Map[String, util.List[OrderEvent]], context: PatternProcessFunction.Context): Unit = {
      // 处理部分匹配的超时事件
      val createEvent: OrderEvent = map.get("create").get(0)
      context.output(new OutputTag[String]("timeout"), s"订单${createEvent.orderId}超时未支付！")
    }
  }
}
