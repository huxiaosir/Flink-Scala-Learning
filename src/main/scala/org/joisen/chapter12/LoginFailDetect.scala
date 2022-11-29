package org.joisen.chapter12

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._

import java.util

/**
 * @Author Joisen
 * @Date 2022/11/29 11:03
 * @Version 1.0
 */
// 定义登录事件的样例类
case class LoginEvent(userId: String, ipAddr: String, eventType: String, timestamp: Long)

object LoginFailDetect {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据源
    val stream: KeyedStream[LoginEvent, String] = env.fromElements(
        LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
        LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
        LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
        LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
        LoginEvent("user_2", "192.168.1.29", "success", 6000L),
        LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
        LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
      )
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.userId)

    // 2 定义pattern，检测连续三次登录失败
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("first")
      .where(_.eventType.equals("fail"))
      .next("second").where(_.eventType.equals("fail"))
      .next("third").where(_.eventType.equals("fail"))

    // 3 将模式应用到事件流上，检测匹配的复杂事件
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(stream, pattern)

    // 4 定义处理规则， 将检测到的匹配事件报警输出
    val resStream: DataStream[String] = patternStream.select(new PatternSelectFunction[LoginEvent, String] {
      override def select(map: util.Map[String, util.List[LoginEvent]]): String = {
        // 获取匹配到的复杂事件
        val first: LoginEvent = map.get("first").get(0)
        val second: LoginEvent = map.get("second").get(0)
        val third: LoginEvent = map.get("third").get(0)
        // 包装报警信息输出
        s"${first.userId} 连续三次登录失败！ 登录时间${first.timestamp}、${second.timestamp}、${third.timestamp}"

      }
    })

    resStream.print()

    env.execute()

  }
}
