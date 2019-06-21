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

package com.cloudera.spark.cloud.adl

import com.cloudera.spark.cloud.common.CloudSuiteWithCSVDatasource
import com.cloudera.spark.cloud.operations.LineCount

/**
 * Test the `LineCount` entry point with ADL.
 */
class AdlLineCountSuite extends CloudSuiteWithCSVDatasource with AdlTestSetup {

  init()

  /**
   * set up FS if enabled.
   */
  def init(): Unit = {
    if (enabled) {
      initFS()
      initDatasources()
    }
  }

  override def enabled: Boolean = super.enabled && hasCSVTestFile

  after {
    cleanFilesystemInTeardown()
  }

  ctest("AzureLineCountSuite",
    "Execute the LineCount example") {
    val src = getTestCSVPath()
    val sparkConf = newSparkConf(src)
    sparkConf.setAppName("AzureLineCountSuite")
    assert(0 === new LineCount().action(sparkConf,
      Seq(src.toUri.toString)))
  }

}
