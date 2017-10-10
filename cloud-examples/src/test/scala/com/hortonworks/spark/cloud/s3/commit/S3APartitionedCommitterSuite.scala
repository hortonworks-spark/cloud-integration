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
import com.hortonworks.spark.cloud.commit.{PathOutputCommitProtocol}
import com.hortonworks.spark.cloud.s3.{S3ACommitterConstants, S3AOperations, S3ATestSetup, SparkS3ACommitProtocol}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
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

  private val s3 = filesystem.asInstanceOf[S3AFileSystem]
  private val destDir = testPath(s3, "dataframe-committer")

  ctest(s"Append Partitioned ORC Data",
    s"Write a dataframe as ORC with append=true",
    true) {
    testOneWriteSequence(
      new Path(destDir, s"committer-partition-ORC"),
      "orc",
      PARTITIONED,
      "append",
      true)
  }

  ctest(s"Overwrite Partitioned Parquet Data",
    s"Write a dataframe as parquet with the partitioned committer",
    true) {
    testOneWriteSequence(
      new Path(destDir, s"committer-partitioneparquet"),
      "parquet",
      PARTITIONED,
      "replace",
      false)
  }

  def testOneWriteSequence(
      destDir: Path,
      format: String,
      committerName: String,
      confictMode: String,
      expectAppend: Boolean): Unit = {

    val local = getLocalFS
    val sparkConf = newSparkConf("DataFrames", local.getUri)

    val committerInfo = COMMITTERS_BY_NAME(committerName)

    val factory = committerInfo._2
    hconf(sparkConf, S3ACommitterConstants.S3A_COMMITTER_FACTORY_KEY, factory)
    logInfo(s"Using committer factory $factory with conflict mode $confictMode")
    hconf(sparkConf, S3ACommitterConstants.CONFLICT_MODE, confictMode)
    // force reject
    hconf(sparkConf, PathOutputCommitProtocol.REJECT_FILE_OUTPUT, true)
//    hconf(sparkConf, CommitterConstants.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 3)



    // validate the conf by asserting that the spark conf is bonded
    // to the partitioned committer.
    assert(
      PARQUET_COMMITTER_CLASS ===
      sparkConf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key),
      s"wrong value of ${SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS}")


    // uncomment to fail factory operations and see where they happen
 //    sparkConf.set(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key, "unknown")


    val s3aFS = filesystem.asInstanceOf[S3AFileSystem]
    val dest = new Path(destDir, format)
    rm(s3aFS, dest)
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport
      .getOrCreate()
    // ignore the IDE if it complains: this *is* used.
    import spark.implicits._

    // Write the DS. Configure save mode so the committer gets
    // to decide how to react to invidual partitions, rather than
    // have the entire directory tree determine the outcome.
    def writeDS(sourceData: Dataset[Event]): Unit = {
      logDuration(s"write to $dest in format $format conflict = $confictMode") {
        sourceData
          .write
          .partitionBy("year", "month")
          .mode(SaveMode.Append)
          .format(format).save(dest.toString)
      }
    }

    try {
      val sc = spark.sparkContext
      val conf = sc.hadoopConfiguration
      val numPartitions = 2
      val eventData = Events.events(2017, 2017, 1, 2, 10).toDS()
      val origFileCount = Events.monthCount(2017, 2017, 1, 2) *
        numPartitions
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

      operations.maybeVerifyCommitter(dest,
        Some(committerName),
        Some(committerInfo._1),
        conf,
        Some(origFileCount),
        s"$format:")
      // read back results and verify they match
      validateRowCount(spark, s3aFS, dest, format, numRows)

      // now for the real fun: write into a subdirectory alongside the others
      val newPartition = Events.events(2017, 2017, 10, 12, 10).toDS()
      val newFileCount = Events.monthCount(2017, 2017, 10, 12)
      writeDS(newPartition)
      operations.maybeVerifyCommitter(dest,
        Some(committerName),
        Some(committerInfo._1),
        conf,
        Some(newFileCount),
        s"$format:")

      // now list the files under the system
      val allFiles = listFiles(s3aFS, dest,true).filterNot(
        st => st.getPath.getName.startsWith("_")).toList

      val currentFileCount = newFileCount + origFileCount
      assert(currentFileCount === allFiles.length,
        s"File count in $allFiles")

      // then write atop the existing files.
      // here the failure depends on what the policy was
      writeDS(newPartition)

      val allFiles2 = listFiles(s3aFS, dest, true).filterNot(
        st => st.getPath.getName.startsWith("_")).toList

      var finalCount = currentFileCount
      if (expectAppend) {
        finalCount += newFileCount
      }
      assert(finalCount === allFiles2.length, s"Final file count in $allFiles2")

    } finally {
      spark.close()
    }

  }

}
