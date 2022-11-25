package org.joisen.chapter05

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import java.sql.PreparedStatement
import java.util

/**
 * @Author Joisen
 * @Date 2022/11/21 16:23
 * @Version 1.0
 */
object SinkToMySQLTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Bob", "./cart", 3000L),
      Event("Alice", "./cart", 3000L),
      Event("Mary", "./prod?id=2", 4000L),
      Event("Mary", "./prod?id=3", 6000L),
      Event("Mary", "./prod?id=4", 5000L))

    stream.addSink( JdbcSink.sink(
      "insert into clicks (user, url) values(?, ?)", // 定义写入MySQL的语句
      new JdbcStatementBuilder[Event] {
        override def accept(t: PreparedStatement, u: Event): Unit = {
          t.setString(1, u.user)
          t.setString(2, u.url)
        }
      },
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://hadoop102:3306/test")
        .withDriverName("com.mysql.jdbc.Driver")
        .withUsername("root")
        .withPassword("999719")
        .build()
    ) )

    env.execute()

  }

}
