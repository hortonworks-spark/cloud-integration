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

package com.hortonworks.spark.cloud.s3

import com.hortonworks.spark.cloud.CloudSuite
import org.apache.hadoop.fs.FileSystem

import org.apache.spark.sql.SparkSession

class S3ACommitterSuite extends CloudSuite with S3ATestSetup {

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
    }
  }

  ctest("SimpleOutput", "generate simple output for ease of breakpoints") {
    val local = FileSystem.getLocal(getConf)
    val sparkConf = newSparkConf(local.getUri)
    sparkConf.setAppName("DataFrames")
    val destDir = testPath(filesystem, "committer")
    filesystem.delete(destDir, true)
    val spark = SparkSession
      .builder
      .appName("DataFrames")
      .config(sparkConf)
      .enableHiveSupport
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val numRows = 10
    val sourceData = spark.range(0, numRows).map(i => i)
    val format = "orc"
    duration(s"write to $destDir in format $format") {
      sourceData.write.format(format).save(destDir.toString)
    }
    val operations = new S3AOperations(filesystem)
    if (conf.getBoolean(S3GUARD_COMMITTER_TEST_ENABLED, false)) {
      operations.verifyS3Committer(destDir, None)
    }
  }
}
