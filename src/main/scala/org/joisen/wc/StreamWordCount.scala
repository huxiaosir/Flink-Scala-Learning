package org.joisen.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/18 15:40
 * @Version 1.0
 */
// 流处理代码
object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")
    // 创建流处理的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 接受socket数据流
    val textDS: DataStream[String] = env.socketTextStream(host, port)

    // 逐一读取数据，分词之后进行 wordCount
    val wordCountDS: DataStream[(String, Int)] = textDS.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
    // 打印输出
    wordCountDS.print().setParallelism(1) // 设置并行度

    // 执行任务
    env.execute("Stream WordCount")

  }
}
