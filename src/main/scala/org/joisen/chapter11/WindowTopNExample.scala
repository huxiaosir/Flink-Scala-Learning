package org.joisen.chapter11

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.joisen.chapter05.Event

/**
 * @Author Joisen
 * @Date 2022/11/28 12:18
 * @Version 1.0
 */
object WindowTopNExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取数据源，并分配时间戳、生成水位线
    val eventStream = env
      .fromElements(
        Event("Alice", "./home", 1000L),
        Event("Bob", "./cart", 1000L),
        Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
        Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
        Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
        Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
        Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
      )
      .assignAscendingTimestamps(_.timestamp)

    // 创建表环境
    val tableEnv = StreamTableEnvironment.create(env)
    // 将数据流转换成表，并指定时间属性
    val eventTable = tableEnv.fromDataStream(
      eventStream,
      $("user"),
      $("url"),
      $("timestamp").rowtime().as("ts")
      // 将 timestamp 指定为事件时间，并命名为 ts
    )

    // 为方便在 SQL 中引用，在环境中注册表 EventTable
    tableEnv.createTemporaryView("EventTable", eventTable)
    // 定义子查询，进行窗口聚合，得到包含窗口信息、用户以及访问次数的结果表
    val subQuery =
      "SELECT window_start, window_end, user, COUNT(url) as cnt " +
        "FROM TABLE ( " +
        "TUMBLE( TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOUR )) " +
        "GROUP BY window_start, window_end, user "
    // 定义 Top N 的外层查询
    val topNQuery =
      "SELECT * " +
        "FROM (" +
        "SELECT *, " +
        "ROW_NUMBER() OVER ( " +
        "PARTITION BY window_start, window_end " +
        "ORDER BY cnt desc " +
        ") AS row_num " +
        "FROM (" + subQuery + ")) " +
        "WHERE row_num <= 2"
    // 执行 SQL 得到结果表
    val result = tableEnv.sqlQuery(topNQuery)
    tableEnv.toDataStream(result).print()
    env.execute()


  }
}
