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

import java.net.URL

import com.hortonworks.spark.cloud.common.CloudTestKeys._

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuiteLike

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
   * Subclasses may override this for different or configurable test sizes.
   * @return the number of entries in parallelized operations.
   */
  protected def testEntryCount: Int = 10 * scaleSizeFactor

  /**
   * Predicate to declare whether or not scale tests are enabled.
   * Large scale tests MUST use this in their implementation of `enabled`
   * @return true iff the configuration declares that scale tests are enabled.
   */
  protected def isScaleTestEnabled: Boolean = getConf.getBoolean(SCALE_TEST_ENABLED, false)

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

  /**
   * Overlay a set of system properties to a configuration, unless the key
   * is "(unset").
   *
   * @param conf config to patch
   * @param keys list of system properties
   */
  protected def patchTestOption(conf: Configuration, keys: Seq[String]): Unit = {
    keys.foreach(key => getKnownSysprop(key).foreach(v =>
      conf.set(key, v, "system property")))
  }

  /**
   * Get a known sysprop, return None if it was not there or it matched the
   * `unset` value.
   *
   * @param key system property name
   * @return any set value
   */
  protected def getKnownSysprop(key: String): Option[String] = {
    val v = System.getProperty(key)
    if (v == null || v.trim().isEmpty || v.trim == UNSET_PROPERTY) {
      None
    } else {
      Some(v.trim)
    }
  }

  /**
   * Locate a class/resource as a resource URL.
   * This does not attempt to findClass a class, merely verify that it is present.
   *
   * @param resource resource or path of class, such as
   * `org/apache/hadoop/fs/azure/AzureException.class`
   * @return the URL or null
   */
  def locateResource(resource: String): URL = {
    getClass.getClassLoader.getResource(resource)
  }

  /**
   * Overlay a set of system properties to a configuration, unless the key
   * is "(unset").
   *
   * @param conf config to patch
   * @param keys list of system properties
   */
  def overlayConfiguration(conf: Configuration, keys: Seq[String]): Unit = {
    keys.foreach(key => {
      getKnownSysprop(key).foreach(v =>
        conf.set(key, v, "system property")
      )
    })
  }
}
