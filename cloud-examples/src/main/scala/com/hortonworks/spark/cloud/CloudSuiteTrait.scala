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

package com.hortonworks.spark.cloud

import com.hortonworks.spark.cloud.s3.S3AConstants
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FunSuite, FunSuiteLike}

import org.apache.spark.LocalSparkContext

/**
 * A cloud suite.
 * Adds automatic loading of a Hadoop configuration file with login credentials and
 * options to enable/disable tests, and a mechanism to conditionally declare tests
 * based on these details
 */
trait CloudSuiteTrait extends FunSuiteLike
    with CloudTestIntegration {

  /**
   * Determine the scale factor for larger tests.
   */
  private lazy val scaleSizeFactor = getConf.getInt(SCALE_TEST_SIZE_FACTOR,
    SCALE_TEST_SIZE_FACTOR_DEFAULT)

  /**
   * Subclasses may override this for different or configurable test sizes
   * @return the number of entries in parallelized operations.
   */
  protected def testEntryCount: Int = 10 * scaleSizeFactor

  /**
   * A conditional test which is only executed when the suite is enabled,
   * and the `extraCondition` predicate holds.
   * @param name test name
   * @param detail detailed text for reports
   * @param extraCondition extra predicate which may be evaluated to decide if a test can run.
   * @param testFun function to execute
   */
  protected def ctest(
      name: String,
      detail: String = "",
      extraCondition: => Boolean = true)
      (testFun: => Unit): Unit = {
    if (enabled && extraCondition) {
      registerTest(name) {
        logInfo(s"$name\n$detail\n-------------------------------------------")
        testFun
      }
    } else {
      registerIgnoredTest(name) {
        testFun
      }
    }
  }

  protected def ctest(
      name: String,
      extraCondition: => Boolean)
    (testFun: => Unit): Unit = {
    ctest(name, "", extraCondition)(testFun)
  }

  /**
   * Is this test suite enabled?
   * The base class is enabled if the configuration file loaded; subclasses can extend
   * this with extra probes, such as for bindings to an object store.
   *
   * If this predicate is false, then tests defined in `ctest()` will be ignored
   * @return true if the test suite is enabled.
   */
  protected def enabled: Boolean = true


}


