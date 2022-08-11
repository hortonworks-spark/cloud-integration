/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.spark.statistics

import org.apache.hadoop.fs.statistics.{IOStatistics, IOStatisticsSnapshot, IOStatisticsSource}

import org.apache.spark.util.AccumulatorV2

/**
 * An accumulator which collects and aggregates IOStatistics.
 */
class IOStatisticsAccumulator extends AccumulatorV2[IOStatistics, IOStatisticsSnapshot]
    with IOStatisticsSource {

  // the snapshot to accumulate.
  private var iostatistics: IOStatisticsSnapshot = new IOStatisticsSnapshot()

  /**
   * Empty if all the various maps are empty.
   * Not thread safe.
   * @return true if the accumulator is empty.
   */
  override def isZero: Boolean = iostatistics.counters().isEmpty &&
    iostatistics.gauges().isEmpty &&
    iostatistics.maximums().isEmpty &&
    iostatistics.minimums().isEmpty &&
    iostatistics.meanStatistics().isEmpty

  override def copy(): AccumulatorV2[IOStatistics, IOStatisticsSnapshot] = {
    val newAcc = new IOStatisticsAccumulator()
    newAcc.add(this.iostatistics)
    newAcc
  }

  override def reset(): Unit = {
    iostatistics.clear()
  }

  override def add(v: IOStatistics): Unit = iostatistics.aggregate(v)

  override def merge(other: AccumulatorV2[IOStatistics, IOStatisticsSnapshot]): Unit =
    add(other.value)

  override def value: IOStatisticsSnapshot = iostatistics

  override def getIOStatistics: IOStatistics = iostatistics

  def register(name: String): Unit = {
    super.isRegistered

  }

}
