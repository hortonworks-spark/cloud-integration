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

import com.hortonworks.spark.cloud.CloudSuite
import com.hortonworks.spark.cloud.s3.{S3AOperations, S3ATestSetup, SparkS3ACommitter}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem

import org.apache.spark.{SparkConf, SparkScopeWorkarounds}
import org.apache.spark.sql.SparkSession

class S3ACommitDataframeSuite extends CloudSuite with S3ATestSetup {

  import com.hortonworks.spark.cloud.s3.S3ACommitterConstants._

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
    }
  }

  /**
   * Patch up hive for re-use.
   * @param sparkConf configuration to patch
   */
  def addTransientDerbySettings(sparkConf: SparkConf): Unit = {
    val hiveConfig = SparkScopeWorkarounds.tempHiveConfig()
    hconf(sparkConf, hiveConfig)
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
    addTransientDerbySettings(sparkConf)
  }

  private val formats = Seq(/*"orc",*/ "parquet")

  // there's an empty string at the end to aid with commenting out different
  // committers and not have to worry about any trailing commas
  private val committers = Seq(
//    DEFAULT_RENAME,
    DIRECTORY,
    PARTITIONED,
    MAGIC,
    ""
  )
  private val s3 = filesystem.asInstanceOf[S3AFileSystem]
  private val destDir = testPath(s3, "dataframe-committer")
  private val isConsistentFS = isConsistentFilesystemConfig

  committers.filter(!_.isEmpty).foreach { committer =>
    formats.foreach {
      fmt =>
        val commitInfo = COMMITTERS_BY_NAME(committer)
        val compatibleFS = isConsistentFS || !commitInfo._3
        ctest(s"Dataframe+$committer-$fmt",
          s"Write a dataframe to $fmt with the committer $committer",
          compatibleFS) {
          testOneFormat(
            new Path(destDir, s"committer-$committer-$fmt"),
            fmt,
            Some(committer))
        }
    }
  }

  def testOneFormat(destDir: Path,
      format: String,
      committerName: Option[String]): Unit = {

    val local = getLocalFS
    val sparkConf = newSparkConf("DataFrames", local.getUri)
    val committerInfo = committerName.map(COMMITTERS_BY_NAME(_))

    committerInfo.foreach { info =>
      hconf(sparkConf, OUTPUTCOMMITTER_FACTORY_CLASS, info._2)
    }
    val s3 = filesystem.asInstanceOf[S3AFileSystem]
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport
      .getOrCreate()
    // ignore the IDE if it complains: this *is* used.
    import spark.implicits._
    try {
      val sc = spark.sparkContext
      val conf = sc.hadoopConfiguration
      val numRows = 500
      val numPartitions = 1
      val sourceData = spark.range(0, numRows)
        .map(i => (1, i, i.toString))
        .repartition(numPartitions)
      val subdir = new Path(destDir, format)
      rm(s3, subdir)
      duration(s"write to $subdir in format $format") {
        sourceData.write.format(format).save(subdir.toString)
      }
      val operations = new S3AOperations(s3)
      val stats = operations.getStorageStatistics()

      logDebug(s"Statistics = \n" + stats.mkString("  ", " = ", "\n"))

      operations.maybeVerifyCommitter(subdir,
        committerName,
        committerInfo.map(_._1),
        conf,
        Some(numPartitions),
        s"$format:")
      // read back results and verify they match
      validateRowCount(spark, s3, subdir, format, numRows)
    } finally {
      spark.close()
    }

  }

}
