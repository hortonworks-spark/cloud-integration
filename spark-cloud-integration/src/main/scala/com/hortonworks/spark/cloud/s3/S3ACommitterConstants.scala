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

import com.hortonworks.spark.cloud.commit.CommitterConstants
import org.apache.hadoop.fs.s3a.commit.CommitConstants

/**
 * Constants related to the S3A committers.
 * Originally a copy & paste of the java values, it's now just a reference,
 * though retained to reserve the option of moving back to copied values.
 */
object S3ACommitterConstants {

  val S3A_COMMITTER_FACTORY_KEY: String = String.format(
    CommitterConstants.OUTPUTCOMMITTER_FACTORY_SCHEME_PATTERN,
    "s3a")
  val STAGING_PACKAGE = "org.apache.hadoop.fs.s3a.commit.staging."
  val DIRECTORY_COMMITTER_FACTORY: String = CommitConstants.DIRECTORY_COMMITTER_FACTORY
    STAGING_PACKAGE + "DirectoryStagingCommitterFactory"
  val PARTITIONED_COMMITTER_FACTORY: String = CommitConstants.PARTITION_COMMITTER_FACTORY
  val STAGING_COMMITTER_FACTORY: String = CommitConstants.STAGING_COMMITTER_FACTORY
  val MAGIC_COMMITTER_FACTORY: String = CommitConstants.MAGIC_COMMITTER_FACTORY
  val DYNAMIC_COMMITTER_FACTORY: String = CommitConstants.DYNAMIC_COMMITTER_FACTORY

  val MAGIC = "magic"
  val STAGING = "staging"
  val DYNAMIC = "dynamic"
  val DIRECTORY = "directory"
  val PARTITIONED = "partitioned"
  val FILE = "file"

  val CONFLICT_MODE: String =
    CommitConstants.FS_S3A_COMMITTER_STAGING_CONFLICT_MODE

  /** Conflict mode */
  val CONFLICT_MODE_FAIL: String = "fail"

  val CONFLICT_MODE_APPEND: String = "append"

  val CONFLICT_MODE_REPLACE: String = "replace"

  /**
   * Committer name to: name in _SUCCESS, factory classname, requires consistent FS.
   *
   * If the first field is "", it means "this committer doesn't put its name into
   * the success file (or that it isn't actually created).
   */
  val COMMITTERS_BY_NAME: Map[String, (String, String, Boolean)] = Map(
    MAGIC -> ("MagicS3GuardCommitter",  MAGIC_COMMITTER_FACTORY, true),
    STAGING -> ("StagingCommitter",  STAGING_COMMITTER_FACTORY, false),
    DYNAMIC -> ("",  DYNAMIC_COMMITTER_FACTORY, false),
    DIRECTORY -> ("DirectoryStagingCommitter",  DIRECTORY_COMMITTER_FACTORY, false),
    PARTITIONED -> ("PartitionedStagingCommitter",  PARTITIONED_COMMITTER_FACTORY, false),
    FILE -> ("", CommitterConstants.DEFAULT_COMMITTER_FACTORY, true)
  )

  /**
   * List of committer factories.
   */
  val FACTORIES: Seq[String] = Seq(
    STAGING_COMMITTER_FACTORY,
    PARTITIONED_COMMITTER_FACTORY,
    MAGIC_COMMITTER_FACTORY,
    DYNAMIC_COMMITTER_FACTORY)
}
