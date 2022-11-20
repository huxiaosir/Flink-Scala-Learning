package org.joisen.chapter05

import org.apache.flink.api.common.functions.{FilterFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/20 15:52
 * @Version 1.0
 */
object TransformReduceTest {
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

    // reduce归约聚合, 获得最活跃的用户
    stream.map( data => (data.user, 1L) )
      .keyBy(_._1)
      .reduce( new MySum() ) // 统计每个用户的活跃度
      .keyBy(data => true) // 将所有数据按照同样的key分到同一个组中
      .reduce( (state, data) => if(data._2 > state._2) data else state ) // 选取最活跃的用户
      .print() // 每流过一条数据 都会有输出，输出为当前的最活跃用户

    env.execute()
  }

  class MySum extends ReduceFunction[(String, Long)]{
    override def reduce(t: (String, Long), t1: (String, Long)): (String, Long) = {
      (t._1, t._2+t1._2)
    }
  }

}
