package org.joisen.chapter12

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration
import java.util

/**
 * @Author Joisen
 * @Date 2022/11/29 11:03
 * @Version 1.0
 */
// 定义登录事件的样例类
case class LoginEvent1(userId: String, ipAddr: String, eventType: String, timestamp: Long)

object LoginFailDetectPro {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据源
    val stream: DataStream[LoginEvent1] = env.fromElements(
      LoginEvent1("user_1", "192.168.0.1", "fail", 2000L),
      LoginEvent1("user_1", "192.168.0.2", "fail", 3000L),
      LoginEvent1("user_2", "192.168.1.29", "fail", 4000L),
      LoginEvent1("user_1", "171.56.23.10", "fail", 5000L),
      LoginEvent1("user_2", "192.168.1.29", "fail", 7000L),
      LoginEvent1("user_2", "192.168.1.29", "fail", 8000L),
        LoginEvent1("user_2", "192.168.1.29", "success", 6000L)
      ).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
      .withTimestampAssigner(new SerializableTimestampAssigner[LoginEvent1] {
        override def extractTimestamp(t: LoginEvent1, l: Long): Long = t.timestamp
      }))


    // 2 定义pattern，检测连续三次登录失败
    val pattern: Pattern[LoginEvent1, LoginEvent1] = Pattern.begin[LoginEvent1]("fail").where(_.eventType.equals("fail")).times(3).consecutive().within(Time.seconds(10))

    // 3 将模式应用到事件流上，检测匹配的复杂事件
    val patternStream: PatternStream[LoginEvent1] = CEP.pattern(stream.keyBy(_.userId), pattern)

    // 4 定义处理规则， 将检测到的匹配事件报警输出
    val resStream: DataStream[String] = patternStream.process(new PatternProcessFunction[LoginEvent1, String] {
      override def processMatch(map: util.Map[String, util.List[LoginEvent1]], context: PatternProcessFunction.Context, collector: Collector[String]): Unit = {
        // 获取匹配到的复杂事件
        val first: LoginEvent1 = map.get("fail").get(0)
        val second: LoginEvent1 = map.get("fail").get(1)
        val third: LoginEvent1 = map.get("fail").get(2)
        // 包装报警信息输出
        collector.collect(s"${first.userId} 连续三次登录失败！ 登录时间${first.timestamp}、${second.timestamp}、${third.timestamp}")

      }
    })

    resStream.print()

    env.execute()

  }
}
