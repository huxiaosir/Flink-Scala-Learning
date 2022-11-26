package org.joisen.chapter11

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.joisen.chapter05.Event

/**
 * @Author Joisen
 * @Date 2022/11/26 10:42
 * @Version 1.0
 */
object SimpleTableExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据，创建DataStream
    val eventStream: DataStream[Event] = env.fromElements(
      Event("Alice", "./home", 1000L),
      Event("Bob", "./cart", 1000L),
      Event("Alice", "./prod?id=1", 5 * 1000L),
      Event("Cary", "./home", 60 * 1000L),
      Event("Bob", "./prod?id=3", 90 * 1000L),
      Event("Alice", "./prod?id=7", 105 * 1000L)
    )

    // 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 将DataStream转换成表
    val eventTable: Table = tableEnv.fromDataStream(eventStream)

    // 调用table Api进行转换计算
    val resTable: Table = eventTable.select($("url"), $("user"))
      .where($("user").isEqual("Alice"))

    // 直接写SQL
    val resSqlTable: Table = tableEnv.sqlQuery("select url, user from " + eventTable + " where user = 'Bob'")


    // 转换成流 打印输出
    tableEnv.toDataStream(resTable).print("Table")
    tableEnv.toDataStream(resSqlTable).print("SQL")

    env.execute()
  }
}
