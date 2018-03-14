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

package com.hortonworks.spark.cloud.s3.commit


import com.hortonworks.spark.cloud.ObjectStoreConfigurations
import org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory
import org.apache.hadoop.fs.s3a.commit.staging.{DirectoryStagingCommitterFactory, PartitionedStagingCommitterFactory}

import org.apache.spark.SparkConf

class S3ACommitterFactorySuite extends AbstractCommitterSuite {

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
    }
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
    sparkConf.setAll(ObjectStoreConfigurations.COMMITTER_OPTIONS)
  }

  ctest("DirectoryStagingCommitterFactory on CP") {
    new DirectoryStagingCommitterFactory()
  }

  ctest("PartitionedStagingCommitterFactory on CP") {
    new PartitionedStagingCommitterFactory()
  }

  ctest("MagicS3GuardCommitterFactory on CP") {
    new MagicS3GuardCommitterFactory()
  }

  ctest("S3ACommitterFactory on CP") {
    new S3ACommitterFactory()
  }


}
