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

import com.hortonworks.spark.cloud.ObjectStoreExample
import com.hortonworks.spark.cloud.s3.S3AConstants._

import org.apache.spark.SparkConf

/**
 * Base Class for examples working with S3.
 */
private[cloud] trait S3AExampleSetup extends ObjectStoreExample {

  /**
   * Set the standard S3A Hadoop options to be used in test/examples.
   * If Random IO is expected, then the experimental fadvise option is
   * set to random.
   *
   * @param sparkConf spark configuration to patch
   * @param randomIO is the IO expected to be random access?
   */
  override protected def applyObjectStoreConfigurationOptions(
      sparkConf: SparkConf, randomIO: Boolean): Unit = {
    super.applyObjectStoreConfigurationOptions(sparkConf, true)
    // smaller block size to divide up work
    hconf(sparkConf, BLOCK_SIZE, 1 * 1024 * 1024)
    hconf(sparkConf, MULTIPART_SIZE, MIN_PERMITTED_MULTIPART_SIZE)
    hconf(sparkConf, READAHEAD_RANGE, "128K")
    hconf(sparkConf, MIN_MULTIPART_THRESHOLD, MIN_PERMITTED_MULTIPART_SIZE)
    hconf(sparkConf, INPUT_FADVISE, if (randomIO) RANDOM_IO else NORMAL_IO)
  }
}
