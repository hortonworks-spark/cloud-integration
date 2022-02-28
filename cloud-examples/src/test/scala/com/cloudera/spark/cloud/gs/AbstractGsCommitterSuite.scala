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

package com.cloudera.spark.cloud.gs

import com.cloudera.spark.cloud.ObjectStoreConfigurations
import com.cloudera.spark.cloud.common.CloudSuite



import org.apache.spark.{SparkConf, SparkScopeWorkarounds}

abstract class AbstractGsCommitterSuite extends CloudSuite with GsTestSetup {
  /**
   * Patch up hive for re-use.
   *
   * @param sparkConf configuration to patch
   */
  def addTransientDerbySettings(sparkConf: SparkConf): Unit = {
    hconf(sparkConf, SparkScopeWorkarounds.tempHiveConfig())
  }

  /**
   * Override point for suites: a method which is called
   * in all the `newSparkConf()` methods.
   * This can be used to alter values for the configuration.
   * It is called before the configuration read in from the command line
   * is applied, so that tests can override the values applied in-code.
   *
   * @param sparkConf spark configuration to alter
   */
  override protected def addSuiteConfigurationOptions(sparkConf: SparkConf): Unit = {
    super.addSuiteConfigurationOptions(sparkConf)
    logDebug("Patching spark conf with committer bindings")
    sparkConf.setAll(ObjectStoreConfigurations.COMMITTER_OPTIONS)
    addTransientDerbySettings(sparkConf)
  }

}
