package org.joisen.chapter06

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, TimestampAssignerSupplier, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.joisen.chapter05.Event

import java.time.Duration


/**
 * @Author Joisen
 * @Date 2022/11/21 20:39
 * @Version 1.0
 */
object WaterMarkTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(500L)

    val stream: DataStream[Event] = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Bob", "./cart", 3000L),
      Event("Alice", "./cart", 3000L),
      Event("Mary", "./prod?id=2", 4000L),
      Event("Mary", "./prod?id=3", 6000L),
      Event("Mary", "./prod?id=4", 5000L))

    // 1 有序流的水位线生成策略
    stream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[Event]()
      .withTimestampAssigner(
        new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
        }
      )
    )

    // 2 乱序流的水位线生成策略
    stream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Event](Duration.ofSeconds(2))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
        }
      )
    )

    // 自定义水位线的生成
    stream.assignTimestampsAndWatermarks( new WatermarkStrategy[Event] {

      override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[Event] = {
        new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(t: Event, l: Long): Long = t.timestamp
        }
      }

      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[Event] = {
        new WatermarkGenerator[Event] {
          // 定义一个延迟时间
          val delay = 5000L
          // 定义属性保存最大时间戳
          var maxTs = Long.MinValue + delay + 1

          override def onEvent(t: Event, l: Long, watermarkOutput: WatermarkOutput): Unit = {
            maxTs = math.max(maxTs, t.timestamp)
          }

          override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {
            val watermark = new Watermark(maxTs - delay - 1)
            watermarkOutput.emitWatermark(watermark)
          }
        }

      }
    })



  }
}
