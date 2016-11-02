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

package org.apache.spark.cloud.examples.docexamples

import org.apache.hadoop.fs.Path

import org.apache.spark.cloud.CloudSuite
import org.apache.spark.cloud.azure.AzureTestSetup


/**
 * Test Azure and DataFrames
 */
private[cloud] class AzureStreamingExampleSuite extends CloudSuite with AzureTestSetup {

  init()

  def init(): Unit = {
    if (enabled) {
      initFS()
    }
  }

  /**
   * Override point: the data frame operation to execute
   */
  ctest("Streaming",
    "Streaming IO") {
    val conf = newSparkConf()
    conf.setAppName("Streaming")
    val destDir = filesystem.makeQualified(new Path("/streaming"))
    val instance = new AzureStreamingExample()
    val args = Seq(destDir, "60" , "10")
    assert(0 === instance.action(conf, args),
      s" action($args) failed against $instance")
  }
}
