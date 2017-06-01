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

import com.hortonworks.spark.cloud.CloudSuite
import org.apache.hadoop.fs.s3a.S3AFileSystem

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

abstract class S3ACommitterSuite extends CloudSuite with S3ATestSetup {

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
    sparkConf.setAll(SparkS3ACommitter.BINDING_OPTIONS)
  }

  ctest("propagation",  "verify property passdown") {
    val name = expectSome(getKnownSysprop(S3A_COMMITTER_NAME),
      s"Unset property ${S3A_COMMITTER_NAME}")
    logInfo(s"Committer name is $name")

    val conf = getConf
    assert(getConf.getBoolean(S3A_COMMITTER_TEST_ENABLED, false),
      "committer setup not passed in")
    val committer = expectOptionSet(conf, OUTPUTCOMMITTER_FACTORY_CLASS)
    val cclass = Class.forName(committer)
    logInfo(s"Committer is $cclass")
  }

  /**
   * This is the least complex of the output writers, the original RDD
   * API.
   */
  ctest("saveAsNewAPIHadoopFile",
    "Write output via the RDD saveAsNewAPIHadoopFile API", true) {

    // store the S3A FS the test is bonded to
    val s3 = filesystem.asInstanceOf[S3AFileSystem]

    // switch to the local FS for staging
    val local = getLocalFS
    setLocalFS();

    val sparkConf = newSparkConf("saveAsFile", local.getUri)
    val destDir = testPath(s3, "saveAsFile")
    s3.delete(destDir, true)
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport
      .getOrCreate()
    val operations = new S3AOperations(s3)
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val committer = expectOptionSet(conf, OUTPUTCOMMITTER_FACTORY_CLASS)

    val numRows = 10

    val sourceData = sc.range(0, numRows).map(i => i)
    val format = "orc"
    duration(s"write to $destDir in format $format") {
      saveAsTextFile(sourceData, destDir, conf, Long.getClass, Long.getClass)
    }
    operations.maybeVerifyCommitter(destDir, None, conf, Some(1),
      s"Saving in format $format:")
  }


}
