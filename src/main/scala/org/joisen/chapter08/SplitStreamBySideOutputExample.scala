package org.joisen.chapter08

/**
 * @Author Joisen
 * @Date 2022/11/23 15:43
 * @Version 1.0
 */
import org.apache.flink.streaming.api.functions._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.joisen.chapter05.{ClickSource, Event}
object SplitStreamBySideOutputExample {
  // 实例化侧输出标签
  val maryTag = OutputTag[(String, String, Long)]("Mary-pv")
  val bobTag = OutputTag[(String, String, Long)]("Bob-pv")
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new ClickSource)
    val processedStream = stream
      .process(new ProcessFunction[Event, Event] {
        override def processElement(value: Event, ctx: ProcessFunction[Event,
          Event]#Context, out: Collector[Event]): Unit = {
          // 将不同的数据发送到不同的侧输出流
          if (value.user == "Mary") {
            ctx.output(maryTag, (value.user, value.url, value.timestamp))
          } else if (value.user == "Bob") {
            ctx.output(bobTag, (value.user, value.url, value.timestamp))
          } else {
            out.collect(value)
          }
        }
      })

    // 打印各输出流中的数据
    processedStream.getSideOutput(maryTag).print("Mary pv")
    processedStream.getSideOutput(bobTag).print("Bob pv")
    processedStream.print("else pv")
    env.execute()
  }
}

