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

package com.cloudera.spark.cloud.utils

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FileSystem, StorageStatistics}

import org.apache.spark.internal.Logging

class StatisticsTracker(fs: FileSystem) extends Logging {

  private val start: StorageStatistics = fs.getStorageStatistics

  import StatisticsTracker._

  val original: Map[String, Long] = statsToMap(start)

  var updated: Map[String, Long] = Map()

  def update(): StatisticsTracker = {
    updated = statsToMap(fs.getStorageStatistics)
    this
  }

  /**
   * Build a diff from current to actual.
   * @return map of changed values only
   */
  def diff(): Map[String, Long] = {
    updated.map { case (name: String, value: Long) =>
      name -> (value - original.getOrElse(name, 0L))
    }.filter{tuple => tuple._2 != 0}
  }

  /**
   * Dump all changed values.
   * @param prefix prefix of a line
   * @param join join between values
   * @param suffix suffix each line
   * @param merge merge between lines
   * @return
   */
  def dump(prefix: String, join: String, suffix: String, merge: String): String = {
    diff.map { case (name, value) =>
      (prefix + name + join + value + suffix)
    }.mkString(merge)

  }

  def dump(): String = {
    fs.getUri + "\n" + dump("  [", " = ", "]", "\n")
  }


}

object StatisticsTracker {

  def statsToMap(stats: StorageStatistics): Map[String, Long] = {

    stats.getLongStatistics.asScala.map { s =>
      s.getName -> s.getValue
    }.toMap

  }

}
