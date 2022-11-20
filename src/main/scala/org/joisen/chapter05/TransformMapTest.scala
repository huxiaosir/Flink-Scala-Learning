package org.joisen.chapter05

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/20 15:43
 * @Version 1.0
 */
object TransformMapTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L))

    // 1 提取每次点击操作的用户名
    stream.map( _.user ).print("1")

    // 2 实现MapFunction接口
    stream.map(new UserExtractor).print("2")

    env.execute()
  }
  // 泛型 T：要操作的数据的类型  O：输出的类型
  class UserExtractor extends MapFunction[Event, String]{
    override def map(t: Event): String = {
      t.user
    }
  }
}
