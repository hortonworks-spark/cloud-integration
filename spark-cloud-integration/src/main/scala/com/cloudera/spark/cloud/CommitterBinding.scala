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

package com.cloudera.spark.cloud

import com.cloudera.spark.cloud.GeneralCommitterConstants.{ABFS_MANIFEST_COMMITTER_FACTORY, DEFAULT_COMMITTER_FACTORY, MANIFEST_COMMITTER_FACTORY, MANIFEST_COMMITTER_NAME}
import org.apache.hadoop.fs.s3a.commit.CommitConstants

/**
 * Constants related to the S3A committers.
 * Originally a copy & paste of the java values, it's now just a reference,
 * though retained to reserve the option of moving back to copied values.
 */
object CommitterBinding {

  def factoryForSchema(s: String): String =
    String.format(
      GeneralCommitterConstants.OUTPUTCOMMITTER_FACTORY_SCHEME_PATTERN,
      s)


  val S3A_SCHEME_COMMITTER_FACTORY: String = factoryForSchema("s3a")
  val STAGING_PACKAGE = "org.apache.hadoop.fs.s3a.commit.staging."
  val S3A_COMMITTER_FACTORY: String = CommitConstants.S3A_COMMITTER_FACTORY

  val S3A_COMMITTER_NAME: String = CommitConstants.FS_S3A_COMMITTER_NAME

  val MAGIC = "magic"
  val STAGING = "staging"
  val DIRECTORY = "directory"
  val PARTITIONED = "partitioned"
  val MANIFEST = "manifest"
  val MANIFEST_ABFS = "manifest_abfs"
  val FILE = "file"

  val S3A_CONFLICT_MODE: String =
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
  val COMMITTERS_BY_NAME: Map[String, CommitterInfo] = Map(
    MAGIC -> CommitterInfo(MAGIC, S3A_COMMITTER_FACTORY),
    STAGING -> CommitterInfo(STAGING, S3A_COMMITTER_FACTORY),
    DIRECTORY -> CommitterInfo(DIRECTORY, S3A_COMMITTER_FACTORY),
    PARTITIONED -> CommitterInfo(PARTITIONED, S3A_COMMITTER_FACTORY),
    MANIFEST -> CommitterInfo(MANIFEST_COMMITTER_NAME,
      MANIFEST_COMMITTER_FACTORY),
    MANIFEST_ABFS -> CommitterInfo(MANIFEST_COMMITTER_NAME,
      ABFS_MANIFEST_COMMITTER_FACTORY),
    FILE -> CommitterInfo("", DEFAULT_COMMITTER_FACTORY)
  )

}



