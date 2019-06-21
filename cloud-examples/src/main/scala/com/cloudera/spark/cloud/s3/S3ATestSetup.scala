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

import java.net.URI

import com.cloudera.spark.cloud.common.{CloudSuiteTrait, CsvDatasourceSupport}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Trait for S3A tests.
 */
trait S3ATestSetup extends CloudSuiteTrait with RandomIOPolicy with
  CsvDatasourceSupport {

  override def enabled: Boolean = {
    getConf.getBoolean(S3A_TESTS_ENABLED, false) &&
      super.enabled &&
      (!consistentFilesystemOnly || isConsistentFilesystemConfig)

  }

  def consistentFilesystemOnly: Boolean = false

  def isConsistentFilesystemConfig: Boolean =
    getConf.getTrimmed(S3A_CLIENT_FACTORY_IMPL, "") == ""

  def initFS(): FileSystem = {
    setupFilesystemConfiguration(getConf)
    createTestS3AFS
  }

  /**
   * do the work of setting up the S3Test FS
   * @return the filesystem
   */
  protected def createTestS3AFS: FileSystem = {
    val s3aURI = new URI(requiredOption(S3A_TEST_URI))
    logInfo(s"Executing S3 tests against $s3aURI with read policy $inputPolicy")
    createFilesystem(s3aURI)
  }

  /**
   * Override point: set up the configuration for the filesystem.
   * The base implementation sets up buffer directory, block size and IO Policy.
   * @param config configuration to set up
   */
  def setupFilesystemConfiguration(config: Configuration): Unit = {
    config.set(BUFFER_DIR, localTmpDir.getAbsolutePath)
    // a block size of 1MB
    config.set(BLOCK_SIZE, (1024 * 1024).toString)
    // the input policy
    config.set(INPUT_FADVISE, inputPolicy)
  }

  lazy val CSV_TESTFILE: Option[Path] = {
    val pathname = getConf.get(S3A_CSVFILE_PATH, S3A_CSV_PATH_DEFAULT)
    if (!pathname.isEmpty) Some(new Path(pathname)) else None
  }

  /**
   * Predicate to define whether or not there's a CSV file to work with.
   * @return true if the CSV test file is defined.
   */
  override def hasCSVTestFile(): Boolean = CSV_TESTFILE.isDefined

  /**
   * Path to the CSV file's original source
   *
   * @return a path
   */
  override def sourceCSVFilePath: Option[Path] = CSV_TESTFILE
}
