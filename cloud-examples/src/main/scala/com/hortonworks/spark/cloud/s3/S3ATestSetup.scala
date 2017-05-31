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

import java.net.URI

import com.hortonworks.spark.cloud.{CloudSuite, CloudSuiteTrait}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Trait for S3A tests.
 */
trait S3ATestSetup extends CloudSuiteTrait with S3AConstants {

  override def enabled: Boolean = {
    getConf.getBoolean(S3A_TESTS_ENABLED, false) &&
      super.enabled
  }

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
   * Overrride point: set up the configuration for the filesystem.
   * The base implementation sets up buffer directory, block size and IO Policy.
 *
   * @param config configuration to set up
   */
  def setupFilesystemConfiguration(config: Configuration): Unit = {
    config.set(BUFFER_DIR, localTmpDir.getAbsolutePath)
    // a block size of 1MB
    config.set(BLOCK_SIZE, (1024 * 1024).toString)
    // the input policy
    config.set(INPUT_FADVISE, inputPolicy)
    if (useCSVEndpoint) {
      enableCSVEndpoint(config)
    }
  }

  lazy val CSV_TESTFILE: Option[Path] = {
    val pathname = getConf.get(S3A_CSVFILE_PATH, S3A_CSV_PATH_DEFAULT)
    if (!pathname.isEmpty) Some(new Path(pathname)) else None
  }

  /**
   * Set up a configuration so that created filesystems will use the endpoint for the CSV file.
   * @param config configuration to patch.
   */
  def enableCSVEndpoint(config: Configuration): Unit = {
    config.set(ENDPOINT,
      config.get(S3A_CSVFILE_ENDPOINT, S3A_CSVFILE_ENDPOINT_DEFAULT))
  }

  /**
   * Predicate to define whether or not there's a CSV file to work with.
   * @return true if the CSV test file is defined.
   */
  protected def hasCSVTestFile: Boolean = CSV_TESTFILE.isDefined

  /**
   * What input policy to request (Hadoop 2.8+).
   * @return the IO type
   */
  protected def inputPolicy: String = SEQUENTIAL_IO

  /**
   * Should the endpoint for the CSV data be used in the configuration?
   * @return false
   */
  protected def useCSVEndpoint: Boolean = false

  /**
   * Predicate which declares that the test and CSV endpoints are different.
   * Tests which want to read the CSV file and then write to their own FS are not
   * going to work -the spark context only has a single endpoint option.
   * @param config configuration to probe
   * @return true if the endpoints are different
   */
  protected def testAndCSVEndpointsDifferent(config: Configuration): Boolean = {
    val generalEndpoint = config.get(ENDPOINT, S3A_CSVFILE_ENDPOINT_DEFAULT)
    val testEndpoint = config.get(S3A_CSVFILE_ENDPOINT, S3A_CSVFILE_ENDPOINT_DEFAULT)
    generalEndpoint != testEndpoint
  }
}
