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

package com.hortonworks.spark.cloud.commit

/**
 * Constants general to all the committers.
 */
object CommitterConstants {

  /**
   * Scheme prefix for per-filesystem scheme committers.
   */
  val OUTPUTCOMMITTER_FACTORY_SCHEME = "mapreduce.outputcommitter.factory.scheme"

  /**
   * String format pattern for per-filesystem scheme committers.
   */
  val OUTPUTCOMMITTER_FACTORY_SCHEME_PATTERN: String =
    OUTPUTCOMMITTER_FACTORY_SCHEME + ".%s"

  /**
   * Name of the configuration option used to configure the
   * output committer factory to use unless there is a specific
   * one for a schema
   */
  val OUTPUTCOMMITTER_FACTORY_CLASS = "mapreduce.pathoutputcommitter.factory.class"

  val DEFAULT_COMMITTER_FACTORY =
    "org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory"

  val BINDING_PATH_OUTPUT_COMMITTER_CLASS =
    "com.hortonworks.spark.cloud.BindingPathOutputCommitter"

  val BINDING_PARQUET_OUTPUT_COMMITTER_CLASS =
    "com.hortonworks.spark.cloud.commit.BindingParquetOutputCommitter"

  val MR_ALGORITHM_VERSION = "mapreduce.fileoutputcommitter.algorithm.version"
  val MR_COMMITTER_CLEANUPFAILURES_IGNORED = "mapreduce.fileoutputcommitter.cleanup-failures.ignored"

  val SUCCESSFUL_JOB_OUTPUT_DIR_MARKER = "mapreduce.fileoutputcommitter.marksuccessfuljobs"
  val FILEOUTPUTCOMMITTER_ALGORITHM_VERSION = "mapreduce.fileoutputcommitter.algorithm.version"
  val FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT = 2
  // Skip cleanup _temporary folders under job's output directory
  val FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED = "mapreduce.fileoutputcommitter.cleanup.skipped"

  /**
   * This is the "Pending" directory of the FileOutputCommitter;
   * data written here is, in that algorithm, renamed into place.
   */
  val TEMP_DIR_NAME = "_temporary"
  /**
   * Marker file to create on success.
   */
  val SUCCESS_FILE_NAME = "_SUCCESS"

}
