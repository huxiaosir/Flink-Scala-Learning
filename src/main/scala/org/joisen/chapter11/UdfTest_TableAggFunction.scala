package org.joisen.chapter11

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.{$, call}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.{AggregateFunction, TableAggregateFunction}
import org.apache.flink.util.Collector

import java.sql.Timestamp


/**
 * @Author Joisen
 * @Date 2022/11/28 16:30
 * @Version 1.0
 */
object UdfTest_TableAggFunction {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    // 1 创建表
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

    // 2 注册表聚合函数
    tableEnv.createTemporarySystemFunction("top2", classOf[Top2])

    // 3 调用函数进行查询转换
    // 首先进行窗口聚合得到cnt值
    val urlCountWindowTable: Table = tableEnv.sqlQuery(
      """
        |select uid, count(url) as cnt, window_start as wstart, window_end as wend
        |from TABLE (
        |   TUMBLE(table eventTable, descriptor(et), interval '1' hour)
        |)
        |group by uid, window_start, window_end
        |""".stripMargin
    )
    // 使用table api 调用表聚合函数
    val resTb: Table = urlCountWindowTable.groupBy($("wend"))
      .flatAggregate(call("top2", $("uid"), $("cnt"), $("wstart"), $("wend")))
      .select($("uid"), $("rank"), $("cnt"), $("window_end"))
    // 4 将结果转换成流并打印输出
    tableEnv.toChangelogStream(resTb).print()

    env.execute()
  }
  // 定义输出结果和中间累加器的样例类
  case class Top2Result(uid: String, window_start: Timestamp, window_end: Timestamp, cnt: Long, rank: Int)

  case class Top2Accumulator(var maxCount: Long, var secondMaxCount: Long, var uid1: String, var uid2: String, var window_start: Timestamp, var window_end: Timestamp)

  // 实现自定义的表聚合函数
  class Top2 extends TableAggregateFunction[Top2Result, Top2Accumulator] {
    override def createAccumulator(): Top2Accumulator = Top2Accumulator(Long.MinValue, Long.MinValue,null,null,null, null)

    // 每来一条数据，需要使用accumulate进行聚合
    def accumulate(acc: Top2Accumulator, uid: String, cnt: Long, window_start: Timestamp, window_end: Timestamp): Unit =  {
      acc.window_start = window_start
      acc.window_end = window_end
      // 判断当前count值是否排名前2位
      if(cnt > acc.maxCount){
        // 名次依次向后顺延
        acc.secondMaxCount = acc.maxCount
        acc.uid2 = acc.uid1
        acc.maxCount = cnt
        acc.uid1 = uid
      }else if(cnt > acc.secondMaxCount){
        acc.secondMaxCount = cnt
        acc.uid2 = uid
      }
    }

    // 输出结果数据
    def emitValue(acc: Top2Accumulator, out: Collector[Top2Result]): Unit ={
      // 判断cnt值是否为初始值
      if(acc.maxCount != Long.MinValue){
        out.collect(Top2Result(acc.uid1, acc.window_start,acc.window_end,acc.maxCount, 1))
      }
      if(acc.secondMaxCount != Long.MinValue){
        out.collect(Top2Result(acc.uid2, acc.window_start, acc.window_end, acc.secondMaxCount, 2))
      }
    }

  }

}
