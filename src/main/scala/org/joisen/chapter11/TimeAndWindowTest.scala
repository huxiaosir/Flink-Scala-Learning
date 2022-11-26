package org.joisen.chapter11

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.joisen.chapter05.Event

import java.time.Duration

/**
 * @Author Joisen
 * @Date 2022/11/26 11:18
 * @Version 1.0
 */
object TimeAndWindowTest {
  def main(args: Array[String]): Unit = {
    // 1 创建表环境
    // 1.1 直接基于流执行环境创建
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    // 1 在创建表的DDL中指定时间属性
    tableEnv.executeSql("create table eventTable("+
      "uid string," +
      "url string," +
      "ts bigint," +
      " et as TO_TIMESTAMP( FROM_UNIXTIME(ts/10000))," +
      " WATERMARK FOR et AS et - INTERVAL '2' SECOND" +
      ") with(" +
      " 'connector' = 'filesystem', " +
      " 'path' = 'input/clicks.txt', " +
      " 'format' = 'csv' " +
      ")" )

    // 2 在将流转换成表的时候指定时间属性字段
    val eventStream: DataStream[Event] = env.fromElements(
      Event("Alice", "./home", 1000L),
      Event("Bob", "./cart", 1000L),
      Event("Alice", "./prod?id=1", 5 * 1000L),
      Event("Cary", "./home", 60 * 1000L),
      Event("Bob", "./prod?id=3", 90 * 1000L),
      Event("Alice", "./prod?id=7", 105 * 1000L)
    ).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
    .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
      override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
    }))
    // 将DataStream转换成表
//    val eventTable: Table = tableEnv.fromDataStream(eventStream, $("url"), $("user").as("uid")
//      ,$("timestamp").as("ts"), $("et").rowtime())

    val eventTable: Table = tableEnv.fromDataStream(eventStream, $("url"), $("user").as("uid")
      , $("timestamp").rowtime().as("ts"))


    tableEnv.from("eventTable").printSchema()
    eventTable.printSchema()
  }

}
