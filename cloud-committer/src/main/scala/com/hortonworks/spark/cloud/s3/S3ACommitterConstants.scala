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

import com.hortonworks.spark.cloud.PathOutputCommitProtocol

object S3ACommitterConstants {

  val OUTPUTCOMMITTER_FACTORY_CLASS = PathOutputCommitProtocol.OUTPUTCOMMITTER_FACTORY_CLASS;

  val DEFAULT_COMMITTER_FACTORY =
    "org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory"

  val STAGING_PACKAGE = "org.apache.hadoop.fs.s3a.commit.staging."
  val DIRECTORY_COMMITTER_FACTORY = STAGING_PACKAGE + "DirectoryStagingCommitterFactory"
  val PARTITIONED_COMMITTER_FACTORY = STAGING_PACKAGE + "PartitonedStagingCommitterFactory"
  val STAGING_COMMITTER_FACTORY = STAGING_PACKAGE + "StagingCommitterFactory"
  val MAGIC_COMMITTER_FACTORY = "org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory"
  val DYNAMIC_COMMITTER_FACTORY = "org.apache.hadoop.fs.s3a.commit.DynamicCommitterFactory"

  val MAGIC = "magic"
  val STAGING = "staging"
  val DYNAMIC = "dynamic"
  val DIRECTORY = "directory"
  val PARTITIONED = "partitioned"
  val DEFAULT_RENAME = "default"

  /**
   * Committer name to: name in _SUCCESS, factory classname, requires consistent FS.
   */
  val COMMITTERS_BY_NAME: Map[String, (String, String, Boolean)] = Map(
    MAGIC -> ("MagicS3GuardCommitter",  MAGIC_COMMITTER_FACTORY, true),
    STAGING -> ("StagingCommitter",  STAGING_COMMITTER_FACTORY, false),
    DYNAMIC -> ("",  DYNAMIC_COMMITTER_FACTORY, false),
    DIRECTORY -> ("DirectoryStagingCommitter",  DIRECTORY_COMMITTER_FACTORY, false),
    PARTITIONED -> ("PartitionedStagingCommitter",  PARTITIONED_COMMITTER_FACTORY, false),
    DEFAULT_RENAME -> ("",  DEFAULT_COMMITTER_FACTORY, true)
  )
}
