package org.joisen.chapter05

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @Author Joisen
 * @Date 2022/11/21 16:23
 * @Version 1.0
 */
object SinkToRedisTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[Event] = env.addSource(new ClickSource)

    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPassword("999719").build()
    stream.addSink(new RedisSink[Event](conf, new MyRedisMapper))

    env.execute()

  }

  // 实现RedisMapper接口
  class MyRedisMapper extends RedisMapper[Event]{
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "clicks")


    override def getKeyFromData(t: Event): String = t.user

    override def getValueFromData(t: Event): String = t.url
  }
}
