package org.joisen.wc

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, createTypeInformation}

/**
 * @Author Joisen
 * @Date 2022/11/18 15:29
 * @Version 1.0
 */
// 批处理代码
object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val path: String = "D:\\product\\JAVA\\Project\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDS: DataSet[String] = env.readTextFile(path)

    // 分词之后做count
    val wordCountDS: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_, 1))
      .groupBy(0) // 按照第一个元素做分组
      .sum(1) // 按照第二个位置的元素进行求和
    
    // 打印输出
    wordCountDS.print()


  }
}
