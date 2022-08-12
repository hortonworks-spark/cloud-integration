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

package com.cloudera.spark.cloud.s3.commit

import com.cloudera.spark.cloud.CommitterBinding._
import com.cloudera.spark.cloud.committers.AbstractCommitDataframeSuite
import com.cloudera.spark.cloud.s3.S3ATestSetup

/**
 * Tests different data formats through the committers.
 */
class S3ACommitDataframeSuite
  extends AbstractCommitDataframeSuite with S3ATestSetup {

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
    }
  }

  override def dynamicPartitioning: Boolean = dynamicOverwrite


  override def schema: String = "s3a"


  // there's an empty string at the end to aid with commenting out different
  // committers and not have to worry about any trailing commas
  override def committers: Seq[String] = Seq(
    //    DEFAULT_RENAME,
    DIRECTORY,
    //    PARTITIONED,
    MAGIC,
    ""
  )


}
