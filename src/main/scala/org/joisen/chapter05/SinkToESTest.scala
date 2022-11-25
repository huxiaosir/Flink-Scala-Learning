package org.joisen.chapter05

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import java.util

/**
 * @Author Joisen
 * @Date 2022/11/21 16:23
 * @Version 1.0
 */
object SinkToESTest {
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

    // 定义es集群的主机列表
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102", 9200))
    // 定义一个ES sink function
    val esFun: ElasticsearchSinkFunction[Event] = new ElasticsearchSinkFunction[Event] {
      override def process(t: Event, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val data = new util.HashMap[String, String]()
        data.put(t.user, t.url)
        // 包装要发送的http请求
        val indexRequest: IndexRequest = Requests.indexRequest().index("clicks")
          .source(data)
          .`type`("event")

        // 发送请求
        requestIndexer.add(indexRequest)

      }
    }
    stream.addSink( new ElasticsearchSink.Builder[Event](httpHosts, esFun).build())

    env.execute()

  }

}
