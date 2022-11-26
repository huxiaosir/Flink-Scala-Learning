package org.joisen.chapter11

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @Author Joisen
 * @Date 2022/11/26 11:18
 * @Version 1.0
 */
object CommonApiTest {
  def main(args: Array[String]): Unit = {
    // 1 创建表环境
    // 1.1 直接基于流执行环境创建
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 1.2 传入一个环境配置参数创建
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tableEnvironment: TableEnvironment = TableEnvironment.create(settings)

    // 2 创建表
    tableEnv.executeSql("create table eventTable("+
      "uid string," +
      "url string," +
      "ts bigint" +
      ") with(" +
      " 'connector' = 'filesystem', " +
      " 'path' = 'input/clicks.txt', " +
      " 'format' = 'csv' " +
      ")" )

    // 3 表的查询转换
    // 3.1 SQL
    val resTable: Table = tableEnv.sqlQuery(" select uid, url, ts from eventTable where uid = 'Alice' ")

    // 统计每个用户访问频次
    val urlCountTable: Table = tableEnv.sqlQuery("select uid, count(url) from eventTable group by uid")
    // 创建虚拟表
    tableEnv.createTemporaryView("tempTable", resTable)

    // 3.2 Table API
    val eventTable: Table = tableEnv.from("eventTable")
    val resTable2: Table = eventTable.where($("url").isEqual("./home"))
      .select($("url"), $("uid"), $("ts"))


    // 4 输出表的创建
    tableEnv.executeSql(
      """
        |create table outputTable(
        |user_name string,url string,ts bigint
        |) with(
        |'connector' = 'filesystem',
        |'path' = 'output',
        |'format' = 'csv' )
        |""".stripMargin)

    // 5 将结果表写入输出表中
//    resTable.executeInsert("outputTable")

    // 转换成流 进行打印输出
    tableEnv.toDataStream(resTable).print("result")
    tableEnv.toChangelogStream(urlCountTable).print("count")
    env.execute()
  }

}
