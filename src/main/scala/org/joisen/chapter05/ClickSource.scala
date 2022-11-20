package org.joisen.chapter05

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

import java.util.Calendar
import scala.util.Random

/**
 * @Author Joisen
 * @Date 2022/11/20 11:50
 * @Version 1.0
 */
// 自定义数据源 要想实现并行读取数据 须继承ParallelSourceFunction
class ClickSource extends SourceFunction[Event]{
  // 标志位
  var running = true

  // run()方法：使用运行时上下文对象（SourceContext）向下游发送数据；
  override def run(sourceContext: SourceFunction.SourceContext[Event]): Unit = {
    // 随机数生成器
    val random = new Random()
    // 定义数据随机选择的范围
    val users: Array[String] = Array("Mary", "Alice", "Bob", "Kary")
    val urls: Array[String] = Array("./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2", "./prod?id=3")

    // 用标志位作为循环判断的条件，发送数据
    while(running){
      val event: Event = Event(users(random.nextInt(users.length)), urls(random.nextInt(urls.length)), Calendar.getInstance.getTimeInMillis)
      // 调用sourceContext的方法向下游发送数据
      sourceContext.collect(event)
      Thread.sleep(1000)
    }

  }

  // cancel()方法：通过标识位控制退出循环，来达到中断数据源的效果。
  override def cancel(): Unit = running = false
}
