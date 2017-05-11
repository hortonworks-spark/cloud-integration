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

package com.hortonworks.spark.cloud.common

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.lang.RandomStringUtils

/**
 * Lifted straight from `org.apache.hadoop.test.GenericTestUtils`.
 */
object GenericTestUtils {
  private val sequence = new AtomicInteger
  /**
   * system property for test data: {@value }
   */
  val SYSPROP_TEST_DATA_DIR = "test.build.data"
  /**
   * Default path for test data: {@value }
   */
  val DEFAULT_TEST_DATA_DIR: String = "target" + File.separator + "test" +
    File.separator + "data"

  /**
   * The default path for using in Hadoop path references: {@value }
   */
  val DEFAULT_TEST_DATA_PATH = "target/test/data/"

  /**
   * Generates a process-wide unique sequence number.
   *
   * @return an unique sequence number
   */
  def uniqueSequenceId: Int = sequence.incrementAndGet

  /**
   * Get a temp path. This may or may not be relative; it depends on what the
   * {@link #SYSPROP_TEST_DATA_DIR} is set to. If unset, it returns a path
   * under the relative path {@link #DEFAULT_TEST_DATA_PATH}
   *
   * @param subpath sub path, with no leading "/" character
   * @return a string to use in paths
   */
  def getTempPath(subpath: String): String = {
    var prop = System
      .getProperty(SYSPROP_TEST_DATA_DIR, DEFAULT_TEST_DATA_PATH)
    if (prop.isEmpty) {
      // corner case: property is there but empty
      prop = DEFAULT_TEST_DATA_PATH
    }
    if (!prop.endsWith("/")) prop = prop + "/"
    prop + subpath
  }

  /**
   * Get the (created) base directory for tests.
   *
   * @return the absolute directory
   */
  def getTestDir: File = {
    var prop = System
      .getProperty(SYSPROP_TEST_DATA_DIR, DEFAULT_TEST_DATA_DIR)
    if (prop.isEmpty) {
      // corner case: property is there but empty
      prop = DEFAULT_TEST_DATA_DIR
    }
    val dir = new File(prop).getAbsoluteFile
    dir.mkdirs
    dir
  }

  /**
   * Get an uncreated directory for tests.
   *
   * @return the absolute directory for tests. Caller is expected to create it.
   */
  def getTestDir(subdir: String): File = new File(getTestDir, subdir)
    .getAbsoluteFile

  /**
   * Get an uncreated directory for tests with a randomized alphanumeric
   * name. This is likely to provide a unique path for tests run in parallel
   *
   * @return the absolute directory for tests. Caller is expected to create it.
   */
  def getRandomizedTestDir: File = new File(getRandomizedTempPath)
    .getAbsoluteFile

  /**
   * Get a temp path.
   * @return a string to use in paths
   */
  def getRandomizedTempPath: String = getTempPath(RandomStringUtils
    .randomAlphanumeric(10))
}
