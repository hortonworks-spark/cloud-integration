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

package com.cloudera.spark.cloud.s3

import com.cloudera.spark.cloud.common.CloudSuiteWithCSVDatasource

/**
 * Test the `S3LineCount` entry point.
 */
class S3ALineCountSuite extends CloudSuiteWithCSVDatasource with S3ATestSetup {

  init()

  def init(): Unit = {
    if (enabled) {
      setupFilesystemConfiguration(getConf)
    }
  }

  override def enabled: Boolean = super.enabled && hasCSVTestFile

  ctest("S3ALineCountReadData",
    "Execute the S3ALineCount example with the default values (i.e. no arguments)") {
    val sparkConf = newSparkConf(getTestCSVPath())
    sparkConf.setAppName("S3ALineCountDefaults")
    assert(0 === S3ALineCount.action(sparkConf, Seq()))
  }

}
