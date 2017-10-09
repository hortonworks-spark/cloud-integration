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
import com.hortonworks.spark.cloud.s3.{S3ACommitterConstants, S3AOperations, S3ATestSetup, SparkS3ACommitProtocol}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkScopeWorkarounds}

/**
 * Tests around the partitioned committer and its conflict resolution.
 */
class S3APartitionedCommitterSuite extends CloudSuite with S3ATestSetup {

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
    logDebug("Patching spark conf with s3a committer bindings")
    sparkConf.setAll(COMMITTER_OPTIONS)
    sparkConf.setAll(SparkS3ACommitProtocol.BINDING_OPTIONS)
    addTransientDerbySettings(sparkConf)
  }

  private val formats = Seq(
    "orc"
//    ,
//    "parquet"
  )

  // there's an empty string at the end to aid with commenting out different
  // committers and not have to worry about any trailing commas
  private val committers = Seq(
//    DEFAULT_RENAME,
//    DIRECTORY,
    PARTITIONED,
//    MAGIC,
    ""
  )

  private val s3 = filesystem.asInstanceOf[S3AFileSystem]
  private val destDir = testPath(s3, "dataframe-committer")
  private val isConsistentFS = isConsistentFilesystemConfig

  ctest(s"Partitioned Dataframe+fail",
    s"Write a dataframe as ORC with the partitioned committer") {
    testOneFormat(
      new Path(destDir, s"committer-partitione-ORC"),
      "orc",
      PARTITIONED,
      "fail")
  }

  def testOneFormat(destDir: Path,
      format: String,
      committerName: String,
      confictMode: String): Unit = {

    val local = getLocalFS
    val sparkConf = newSparkConf("DataFrames", local.getUri)

    val committerInfo = COMMITTERS_BY_NAME(committerName)

    val factory = committerInfo._2
    hconf(sparkConf, S3ACommitterConstants.S3A_COMMITTER_FACTORY_KEY, factory)
    logInfo(s"Using committer factory $factory with conflict mode $confictMode")
    hconf(sparkConf, S3ACommitterConstants.CONFLICT_MODE, confictMode)
    val s3aFS = filesystem.asInstanceOf[S3AFileSystem]
    val subdir = new Path(destDir, format)
    rm(s3aFS, subdir)

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport
      .getOrCreate()
    // ignore the IDE if it complains: this *is* used.
    import spark.implicits._

    def writeDS(sourceData: Dataset[Event]): Unit = {
      logDuration(s"write to $subdir in format $format conflict = $confictMode") {
        sourceData
          .write.partitionBy("year", "month")
          .format(format).save(subdir.toString)
      }
    }

    try {
      val sc = spark.sparkContext
      val conf = sc.hadoopConfiguration
      val numPartitions = 1
      val eventData = Events.events(2017, 2017, 1, 2, 10).toDS()
      val sourceData = eventData.repartition(numPartitions).cache()
      sourceData.printSchema()
      val eventCount = sourceData.count()
      logInfo(s"${eventCount} elements")
      sourceData.show(10)
      val numRows = eventCount

      writeDS(sourceData)
      val operations = new S3AOperations(s3aFS)
      val stats = operations.getStorageStatistics()

      logDebug(s"Statistics = \n" + stats.mkString("  ", " = ", "\n"))

      operations.maybeVerifyCommitter(subdir,
        Some(committerName),
        Some(committerInfo._1),
        conf,
        Some(numPartitions),
        s"$format:")
      // read back results and verify they match
      validateRowCount(spark, s3aFS, subdir, format, numRows)

      // now for the real fun: write into a subdirectory alongside the others
      val newPartition = Events.events(2017, 2017, 11, 12, 10).toDS()
      writeDS(newPartition)

    } finally {
      spark.close()
    }

  }

}
