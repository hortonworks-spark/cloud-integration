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

import com.hortonworks.spark.cloud.commit.{CommitterConstants, PathOutputCommitProtocol}
import com.hortonworks.spark.cloud.s3.{S3ACommitterConstants, S3AOperations, S3ATestSetup}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
 * This is a large data workflow, starting with the landsat
 * dataset
 */
class S3ACommitBulkDataSuite extends AbstractCommitterSuite with S3ATestSetup {

  import com.hortonworks.spark.cloud.s3.S3ACommitterConstants._

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
      prepareTestCSVFile()
    }
  }

  override def enabled: Boolean = super.enabled && hasCSVTestFile &&
    isScaleTestEnabled

  private val destFS = filesystem.asInstanceOf[S3AFileSystem]

  private val destDir = testPath(destFS, "bulkdata")

  private val formats = Seq(
    "orc",
    "parquet"
  )

  private val modes = Seq("append", "replace")

  formats.foreach { format =>
    modes.foreach{ mode =>
      ctest(s"Write $format-$mode",
        s"Write a partitioned dataframe in format $format with conflict mode $mode") {
        testOneWriteSequence(
          new Path(destDir, s"$format/$mode"),
          format,
          PARTITIONED,
          mode,
          true)
      }

    }
  }

  /**
   * Test one write sequence
   * @param destDir destination
   * @param format output format
   * @param committerName committer name to use
   * @param confictMode how to deal with conflict
   * @param expectAppend should the output be expected to be appended or overwritten
   */
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
    logInfo(s"Using committer factory $factory with conflict mode $confictMode" +
      s" writing $format data")
    hconf(sparkConf, S3ACommitterConstants.CONFLICT_MODE, confictMode)
    // force reject
    hconf(sparkConf, PathOutputCommitProtocol.REJECT_FILE_OUTPUT, true)

    // force failfast
    hconf(sparkConf, CommitterConstants.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 3)



    // validate the conf by asserting that the spark conf is bonded
    // to the partitioned committer.
    assert(
      PARQUET_COMMITTER_CLASS ===
      sparkConf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key),
      s"wrong value of ${SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS}")


    val dest = new Path(destDir, format)
    rm(destFS, dest)
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
      val operations = new S3AOperations(destFS)
      val stats = operations.getStorageStatistics()

      logDebug(s"Statistics = \n" + stats.mkString("  ", " = ", "\n"))

      operations.maybeVerifyCommitter(dest,
        Some(committerName),
        Some(committerInfo._1),
        conf,
        Some(origFileCount),
        s"$format:")
      // read back results and verify they match
      validateRowCount(spark, destFS, dest, format, numRows)

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
      val allFiles = listFiles(destFS, dest,true).filterNot(
        st => st.getPath.getName.startsWith("_")).toList

      val currentFileCount = newFileCount + origFileCount
      assert(currentFileCount === allFiles.length,
        s"File count in $allFiles")

      // then write atop the existing files.
      // here the failure depends on what the policy was
      writeDS(newPartition)

      val allFiles2 = listFiles(destFS, dest, true).filterNot(
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
