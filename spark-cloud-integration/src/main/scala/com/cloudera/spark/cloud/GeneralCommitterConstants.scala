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

/**
 * Constants in the Hadoop codebase related to committer setup
 * and configuration.
 */
object GeneralCommitterConstants {


  /**
   * Scheme prefix for per-filesystem scheme committers.
   */
  val OUTPUTCOMMITTER_FACTORY_SCHEME: String = "mapreduce.outputcommitter.factory.scheme"

  /**
   * String format pattern for per-filesystem scheme committers.
   */
  val OUTPUTCOMMITTER_FACTORY_SCHEME_PATTERN: String =
    OUTPUTCOMMITTER_FACTORY_SCHEME + ".%s"

  /**
   * Name of the configuration option used to configure the
   * output committer factory to use unless there is a specific
   * one for a schema.
   */
  val OUTPUTCOMMITTER_FACTORY_CLASS: String = "mapreduce.pathoutputcommitter.factory.class"

  val DEFAULT_COMMITTER_FACTORY: String =
    "org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory"

  /**
   * The committer which can be directly instantiated and which then delegates
   * all operations to the factory-created committer it creates itself.
   */
  val BINDING_PATH_OUTPUT_COMMITTER_CLASS: String =
    "org.apache.hadoop.mapreduce.lib.output.BindingPathOutputCommitter"

  val BINDING_PARQUET_OUTPUT_COMMITTER_CLASS: String =
    "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"

  val SUCCESSFUL_JOB_OUTPUT_DIR_MARKER: String = "mapreduce.fileoutputcommitter.marksuccessfuljobs"
  val FILEOUTPUTCOMMITTER_ALGORITHM_VERSION: String = "mapreduce.fileoutputcommitter.algorithm.version"
  val FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT: Int = 2
  // Skip cleanup _temporary folders under job's output directory
  val FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED: String = "mapreduce.fileoutputcommitter.cleanup.skipped"

  /**
   * This is the "Pending" directory of the FileOutputCommitter
   * data written here is, in that algorithm, renamed into place.
   */
  val TEMP_DIR_NAME: String = "_temporary"
  /**
   * Marker file to create on success.
   */
  val SUCCESS_FILE_NAME: String = "_SUCCESS"

  /**
   * Flag to trigger creation of a marker file on job completion.
   */
  val CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER = "mapreduce.fileoutputcommitter.marksuccessfuljobs"


  val PATH_OUTPUT_COMMITTER_NAME: String = "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"

  /**
   * The manifest committer.
   */
  val MANIFEST_COMMITTER_NAME: String = "org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitter"


  /**
   * The UUID for jobs.
   * This was historically created in Spark 1.x's SQL queries, but "went away".
   * It has been restored in recent spark releases.
   * If found: it is used instead of the MR job attempt ID.
   */
  val SPARK_WRITE_UUID: String = "spark.sql.sources.writeJobUUID"

  /**
   * Prefix to use for config options..
   */
  val OPT_PREFIX: String = "mapreduce.manifest.committer."

  val OPT_IO_PROCESSORS: String = OPT_PREFIX + "io.thread.count"

  /**
   * Default value..
   */
  val OPT_IO_PROCESSORS_DEFAULT: Int = 32

  /**
   * Should the output be validated?
   */
  val OPT_VALIDATE_OUTPUT: String = OPT_PREFIX + "validate.output"

  val OPT_VALIDATE_OUTPUT_DEFAULT = false

  /**
   * Name of the factory.
   */
  val MANIFEST_COMMITTER_FACTORY: String =
    "org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory"


  val ABFS_SCHEME_COMMITTER_FACTORY: String =
    String.format(OUTPUTCOMMITTER_FACTORY_SCHEME_PATTERN, "abfs")
}
