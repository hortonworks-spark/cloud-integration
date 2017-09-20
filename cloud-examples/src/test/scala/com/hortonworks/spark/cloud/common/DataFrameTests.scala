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

package com.hortonworks.spark.cloud.common

import com.hortonworks.spark.cloud.CloudSuite
import com.hortonworks.spark.cloud.operations.CloudDataFrames

/**
 * Test dataframe and object store integration
 */
abstract class DataFrameTests extends CloudSuite {

  after {
    cleanFilesystemInTeardown()
  }

  /**
   * Override point: the data frame operation to execute
   */
  protected val instance: CloudDataFrames =  new CloudDataFrames()

  ctest("DataFrames",
    "Execute the Data Frames example") {
    val conf = newSparkConf()
    conf.setAppName("DataFrames")
    val destDir = testPath(filesystem, "dataframes")
    val rowCount = 1000

    val args = Seq(destDir, rowCount)
    assert(0 === instance.action(conf, args),
      s" action($args) failed against $instance")

    // do a recursive listFiles
    val listing = logDuration("listFiles(recursive)") {
      listFiles(filesystem, destDir, true)
    }

    var recursivelyListedFilesDataset = 0L
    var recursivelyListedFiles = 0
    logDuration("scan result list") {
      listing.foreach{status =>
        recursivelyListedFiles += 1
        recursivelyListedFilesDataset += status.getLen
        logInfo(s"${status.getPath}[${status.getLen}]")
      }
    }

    logInfo(s"FileSystem $filesystem")
  }

}
