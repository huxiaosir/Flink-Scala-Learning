package org.joisen.chapter11

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction

/**
 * @Author Joisen
 * @Date 2022/11/28 16:30
 * @Version 1.0
 */
object UdfTest_ScalarFunction {
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
    tableEnv.createTemporarySystemFunction("myHash", classOf[MyHash])

    // 3 调用函数进行查询
    val resTb: Table = tableEnv.sqlQuery("select uid, myHash(uid) from eventTable")

    // 4 将结果转换成流并打印输出
    tableEnv.toDataStream(resTb).print()

    env.execute()
  }

  // 实现自定义的标量函数
  class MyHash extends ScalarFunction{
    def eval(str: String): Int= {
      str.hashCode
    }

  }

}
