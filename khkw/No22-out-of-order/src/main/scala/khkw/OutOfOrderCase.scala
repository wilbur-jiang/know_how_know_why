/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package khkw

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Skeleton code for the Out of order case.
 */
object OutOfOrderCase {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.addSource(new SourceFunction[(String, Long)]() {
      def run(ctx: SourceFunction.SourceContext[(String, Long)]) {
        ctx.collect("key", 0L)
        ctx.collect("key", 1000L)
        ctx.collect("key", 2000L)
        ctx.collect("key", 3000L)
        ctx.collect("key", 3000L)
        ctx.collect("key", 4000L)
        ctx.collect("key", 6000L)
        // out of order
        ctx.collect("key", 4000L)
        ctx.collect("key", 1000L)
        ctx.collect("key", 6000L)
        ctx.collect("key", 6000L)
        ctx.collect("key", 7000L)
        ctx.collect("key", 8000L)
        ctx.collect("key", 10000L)
        // out of order
        ctx.collect("key", 8000L)
        ctx.collect("key", 9000L)

        // source is finite, so it will have an implicit MAX watermark when it finishes
      }
      def cancel() {
      }
    }).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(String, Long)] {

      //            private val outOfOrder = 0
      // Result
      // (key,13000)
      // (key,33000)
      // (key,10000)

      private val outOfOrder = 3000
      // Result
      // (key,18000)
      // (key,50000)
      // (key,10000)

      //      private val outOfOrder = 1000
      // Result
      // (key,13000)
      // (key,50000)
      // (key,10000)
      //何时不再处理outOfOrder的数据？这个问题应该说是何时时间窗口何时可以判断为结束，这里应当以watermark为准，Watermark才是最终的裁判。
      // 当（key,6000）到达时，watermark为5000，触发了第一个时间窗口[0,5000)的计算，之后到达的数据(key,4000),(key,1000)不再参与计算
      // 当 ("key", 10000L)数据流入时，watermark为9000，第二个时间窗口[5000,9000)仍未关闭，因此之后的（key,8000）,(key,9000)均参与计算
      //数据流结束后发送一个Long.MAX_VALUE的Watermark，触发第三个时间窗口的计算

      override def extractTimestamp(element: (String, Long), previousTimestamp: Long): Long = {
        element._2
      }

      def checkAndGetNextWatermark(lastElement: (String, Long), extractedTimestamp: Long): Watermark = {
        val ts = lastElement._2 - outOfOrder
        new Watermark(ts)
      }
    }).keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
      .sum(1).print()

    env.execute("Out of order")
  }
}
