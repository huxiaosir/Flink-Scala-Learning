package org.joisen.chapter05

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/20 15:52
 * @Version 1.0
 */
object TransformFilterTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L))

    // 过滤用户Mary的所有点击事件
    // 方式1：使用 lambda 表达式
    stream.filter(_.user == "Mary").print("1")

    // 方式2：实现FilterFunction
    stream.filter(new UserFilter)

    env.execute()
  }

  class UserFilter extends FilterFunction[Event]{
    override def filter(t: Event): Boolean = t.user == "Bob"
  }

}
