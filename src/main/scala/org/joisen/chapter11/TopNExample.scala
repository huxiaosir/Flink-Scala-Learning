package org.joisen.chapter11

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @Author Joisen
 * @Date 2022/11/28 10:55
 * @Version 1.0
 */
object TopNExample {
  def main(args: Array[String]): Unit = {
    // 1 创建表环境
    // 1.1 直接基于流执行环境创建
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    // 1 在创建表的DDL中指定时间属性
    tableEnv.executeSql("create table eventTable(" +
      "uid string," +
      "url string," +
      "ts bigint," +
      " et as TO_TIMESTAMP( FROM_UNIXTIME(ts/1000))," +
      " WATERMARK FOR et AS et - INTERVAL '2' SECOND" +
      ") with(" +
      " 'connector' = 'filesystem', " +
      " 'path' = 'input/clicks.txt', " +
      " 'format' = 'csv' " +
      ")")

    // TOP N  选取活跃度最大的前2个用户
    // 1. 进行分组聚合统计，计算每个用户目前的访问量
    val urlCountTable: Table = tableEnv.sqlQuery("select uid, count(url) as cnt from eventTable group by uid")
    tableEnv.createTemporaryView("urlCountTable", urlCountTable)
    // 提取count值最大的前2个用户
    val top2ResTbl: Table = tableEnv.sqlQuery(
      """
        |select uid, cnt, row_num
        |from(
        | select *, row_number() over(
        |   order by cnt desc
        | ) as row_num
        | from urlCountTable
        |)
        |where row_num <= 2
        |""".stripMargin)
    tableEnv.toChangelogStream(top2ResTbl).print()
    env.execute()


  }
}
