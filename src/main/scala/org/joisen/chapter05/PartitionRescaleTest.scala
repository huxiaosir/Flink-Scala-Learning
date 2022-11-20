package org.joisen.chapter05

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/20 19:32
 * @Version 1.0
 */
object PartitionRescaleTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取自定义的数据源
    val stream: DataStream[Int] = env.addSource(new RichParallelSourceFunction[Int] {
      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
        for(i <- 0 to 7){
          // 利用运行时上下文中的subTask的id信息，来控制数据由哪个并行子任务生成
          if(getRuntimeContext.getIndexOfThisSubtask == (i + 1) % 2)
            sourceContext.collect(i + 1)
        }
      }

      override def cancel(): Unit = ???
    }).setParallelism(2)

    stream.rescale.print().setParallelism(4)

    env.execute()

  }

}
