package org.joisen.chapter11

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.AggregateFunction

/**
 * @Author Joisen
 * @Date 2022/11/28 16:30
 * @Version 1.0
 */
object UdfTest_AggregateFunction {
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

    // 2 注册标量函数
    tableEnv.createTemporarySystemFunction("weightAvg", classOf[WeightAvg])

    // 3 调用函数进行查询
    val resTb: Table = tableEnv.sqlQuery("select uid, weightAvg(ts, 1) as avg_ts from eventTable group by uid")

    // 4 将结果转换成流并打印输出
    tableEnv.toChangelogStream(resTb).print()

    env.execute()
  }
  // 单独定义样例类，用来表示聚合过程中累加器的类型
  case class WeightedAvgAccumulator(var sum: Long = 0L, var count: Int = 0)

  // 实现自定义的聚合函数，计算加权平均数
  class WeightAvg extends AggregateFunction[java.lang.Long, WeightedAvgAccumulator]{
    override def getValue(acc: WeightedAvgAccumulator): java.lang.Long = {
      if(acc.count == 0) null
      else acc.sum / acc.count
    }

    override def createAccumulator(): WeightedAvgAccumulator = WeightedAvgAccumulator() // 创建累计器

    // 每来一条数据都会调用
    def accumulate(acc: WeightedAvgAccumulator, iValue: java.lang.Long, iWeight: Int): Unit ={
      acc.sum += iValue
      acc.count = iWeight
    }

  }

}
