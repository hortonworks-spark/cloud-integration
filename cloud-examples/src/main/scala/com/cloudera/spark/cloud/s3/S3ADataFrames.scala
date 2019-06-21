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

import com.cloudera.spark.cloud.common.CloudTestKeys
import com.cloudera.spark.cloud.operations.CloudDataFrames
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.SparkSession

/**
 * Test dataframe operations using S3 as the destination and source of operations.
 * This validates the various conversion jobs all work against the object store.
 *
 * It doesn't verify timings, though some information is printed.
 */
object S3ADataFrames extends CloudDataFrames with S3AExampleSetup {

  override def extraValidation(
      session: SparkSession,
      conf: Configuration,
      fs: FileSystem,
      results: Seq[(String, Path, Long, Long)]): Unit = {

    val operations = new S3AOperations(fs)
    if (conf.getBoolean(CloudTestKeys.S3A_COMMITTER_TEST_ENABLED, false)) {
      results.foreach((tuple: (String, Path, Long, Long)) => {
        operations.verifyS3Committer(tuple._2, None, None, "")
      })
    }

  }
}
