package org.joisen.wc

import org.apache.flink.streaming.api.scala._

/**
 * @Author Joisen
 * @Date 2022/11/18 18:44
 * @Version 1.0
 */
object BoundedStreamWordCount {
  def main(args: Array[String]): Unit = {
    // 创建流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 读取文本文件数据
    val path: String = "D:\\product\\JAVA\\Project\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val lineDS: DataStream[String] = env.readTextFile(path)
    // 对数据进行转换处理
    val wordAndOne: DataStream[(String, Int)] = lineDS.flatMap(_.split(" ")).map((_, 1))
    // 按照单词进行分组
    val wordAndOneGroup: KeyedStream[(String, Int), String] = wordAndOne.keyBy(_._1)
    // 对分组数据进行sum聚合统计
    val sum: DataStream[(String, Int)] = wordAndOneGroup.sum(1)

    sum.print()

    // 执行任务
    env.execute()

  }
}
