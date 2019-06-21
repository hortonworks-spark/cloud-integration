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

import com.cloudera.spark.cloud.ObjectStoreConfigurations
import com.cloudera.spark.cloud.common.StoreTestOperations
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf

/**
 * Base Class for examples working with S3.
 */
trait S3AExampleSetup extends StoreTestOperations with S3AConstants {

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
    hconf(sparkConf, FAST_UPLOAD, true)
    // shorter delay than the default, for faster tests
    hconf(sparkConf, FAIL_INJECT_INCONSISTENCY_MSEC, DEFAULT_DELAY_KEY_MSEC)
    // disable file output in the path output committer as s safety check
    hconf(sparkConf, REJECT_FILE_OUTPUT, true)
    verifyConfigurationOptions(sparkConf,
      ObjectStoreConfigurations.COMMITTER_OPTIONS)
  }

  /**
   * Any delay for consistency.
   *
   * @return delay in millis; 0 is default.
   */
  override def consistencyDelay(c: Configuration): Int = {
    if (c.getTrimmed(S3A_CLIENT_FACTORY_IMPL, "") != "") {
      DEFAULT_DELAY_KEY_MSEC
    } else {
      0
    }
  }

}
