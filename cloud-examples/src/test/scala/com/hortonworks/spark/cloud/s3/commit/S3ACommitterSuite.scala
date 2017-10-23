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


import com.hortonworks.spark.cloud.s3.{S3ACommitterConstants, S3AOperations}
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.hadoop.fs.s3a.commit.DynamicCommitterFactory
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory
import org.apache.hadoop.fs.s3a.commit.staging.{DirectoryStagingCommitterFactory, PartitonedStagingCommitterFactory}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class S3ACommitterSuite extends AbstractCommitterSuite {

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
    sparkConf.setAll(COMMITTER_OPTIONS)
  }


  /**
   * This is the least complex of the output writers, the original RDD
   * API.
   */
  ctest("saveAsNewAPIHadoopFile",
    "Write output via the RDD saveAsNewAPIHadoopFile API",
    false) {

    // store the S3A FS the test is bonded to
    val s3 = filesystem.asInstanceOf[S3AFileSystem]

    // switch to the local FS for staging
    val local = getLocalFS
    setLocalFS()

    val sparkConf = newSparkConf("saveAsFile", local.getUri)
    val destDir = testPath(s3, "saveAsFile")
    rm(s3, destDir)
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport
      .getOrCreate()

    val operations = new S3AOperations(s3)
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    expectOptionSet(conf, S3ACommitterConstants.S3A_SCHEME_COMMITTER_FACTORY)

    val numRows = 10

    val sourceData = sc.range(0, numRows).map(i => i)

    spark.createDataFrame(Seq((1, "4"), (2, "2")))

    val format = "orc"
    logDuration(s"write to $destDir in format $format") {
      saveAsTextFile(sourceData, destDir, conf, Long.getClass, Long.getClass)
    }
    operations.maybeVerifyCommitter(destDir,
      None,
      None,
      conf,
      Some(1),
      s"Saving in format $format:")
  }

  ctest("DirectoryStagingCommitterFactory on CP") {
    new DirectoryStagingCommitterFactory()
  }

  ctest("PartitonedStagingCommitterFactory on CP") {
    new PartitonedStagingCommitterFactory()
  }

  ctest("MagicS3GuardCommitterFactory on CP") {
    new MagicS3GuardCommitterFactory()
  }

  ctest("DynamicCommitterFactory on CP") {
    new DynamicCommitterFactory()
  }


}
