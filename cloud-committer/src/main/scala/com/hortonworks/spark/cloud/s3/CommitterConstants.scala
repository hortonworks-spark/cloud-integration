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

object CommitterConstants {

  val OUTPUTCOMMITTER_FACTORY_CLASS = "mapreduce.pathoutputcommitter.factory.class"

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
  val DEFAULT = "default"

  val COMMITTERS_BY_NAME: Map[String, String] = Map(
    MAGIC -> MAGIC_COMMITTER_FACTORY,
    STAGING -> STAGING_COMMITTER_FACTORY,
    DYNAMIC -> DYNAMIC_COMMITTER_FACTORY,
    DIRECTORY -> DIRECTORY_COMMITTER_FACTORY,
    PARTITIONED -> PARTITIONED_COMMITTER_FACTORY,
    DEFAULT -> DEFAULT_COMMITTER_FACTORY
  )
}
