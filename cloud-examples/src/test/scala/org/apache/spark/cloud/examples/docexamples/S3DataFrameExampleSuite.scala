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

import org.apache.spark.cloud.CloudSuite
import org.apache.spark.cloud.s3.S3aTestSetup

/**
 * Test the [S3DataFrames] logic.
 */
private[cloud] class S3DataFrameExampleSuite extends CloudSuite with S3aTestSetup {

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
    }
  }

  /**
   * Override point: the data frame operation to execute
   */
  ctest("DataFrames",
    "Dataframe IO") {
    val conf = newSparkConf()
    conf.setAppName("DataFrames")
    val destDir = testPath(filesystem, "dataframes")
    val instance = new S3DataFrameExample()
    val args = Seq(destDir)
    assert(0 === instance.action(conf, args),
      s" action($args) failed against $instance")
  }

}
