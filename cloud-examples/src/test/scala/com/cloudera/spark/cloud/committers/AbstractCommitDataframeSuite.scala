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

package com.cloudera.spark.cloud.committers

import java.io.IOException

import com.cloudera.spark.cloud.CommitterBinding._
import com.cloudera.spark.cloud.s3.CommitterOperations
import com.cloudera.spark.cloud.s3.commit.{Event, Events}
import com.cloudera.spark.cloud.utils.StatisticsTracker
import com.cloudera.spark.cloud.CommitterInfo
import com.cloudera.spark.cloud.ObjectStoreConfigurations.DYNAMIC_PARTITIONING
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.SparkConf

/**
 * Tests different data formats through the committers.
 */
abstract class AbstractCommitDataframeSuite extends AbstractCommitterSuite {

  /**
   * Formats to test.
   */
  private val formats = Seq(
  //  "orc",
    "parquet",  // just use parquet as it is the fussiest one to commit through.
    ""
  )

  // there's an empty string at the end to aid with commenting out different
  // committers and not have to worry about any trailing commas
  def committers: Seq[String]

  def schema: String

  private lazy val destDir = testPath(filesystem, "dataframe-committer")

  nonEmpty(committers).foreach { committer =>
    nonEmpty(formats).foreach {
      fmt =>
        val commitInfo = COMMITTERS_BY_NAME(committer)

        // generate test for chosen committer and format
        ctest(s"Dataframe+$committer-$fmt",
          s"Write a dataframe to $fmt with the committer $committer") {
          executeDFOperations(
            new Path(destDir, s"committer-$committer-$fmt"),
            fmt,
            committer)
        }
    }
  }

  /**
   * Error string expected from spark if the committer doesn't
   * support dynamic partitions.
   */
  protected val DYNAMIC_PARTITION_UNSUPPORTED = "PathOutputCommitter does not support dynamicPartitionOverwrite"


  /**
   * Execute dataframe operations.
 *
   * @param destDir destination dir
   * @param format file format
   * @param committerName committer to look up and use
   */
  def executeDFOperations(
      destDir: Path,
      format: String,
      committerName: String): Unit = {

    val local = getLocalFS
    val sparkConf = newSparkConf("DataFrames", local.getUri)
    val committerInfo = COMMITTERS_BY_NAME(committerName)


    committerInfo.bindToSchema(sparkConf, schema)

    setDynamicPartitioningOptions(sparkConf, committerInfo)

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport
      .getOrCreate()
    // ignore the IDE if it complains: this *is* used.
    //     import spark.implicits._
    import spark.implicits._

    try {
      val sc = spark.sparkContext
      val conf = sc.hadoopConfiguration
      val numPartitions = 4
      val year = 2017
      val eventData = Events.events(year, year, 1, numPartitions-1, 10).toDS()
      val sourceData = eventData.repartition(numPartitions).cache()
      sourceData.printSchema()
      val eventCount = sourceData.count()
      logInfo(s"${eventCount} elements")
      sourceData.show(10)
      val numRows = eventCount

      val subdir = new Path(destDir, format)
      rm(filesystem, subdir)

      val stats = new StatisticsTracker(filesystem)

      logDuration(s"write to $subdir in format $format") {
        sourceData
          .write
          .partitionBy("year", "month")
          .format(format)
          .save(subdir.toString)
      }
      val operations = new CommitterOperations(filesystem)
      stats.update()

      logDebug(s"Statistics = \n${stats.dump()}")

      operations.verifyCommitter(subdir,
        Some(committerInfo),
        None,
        // Some(numPartitions),
        s"$format:")
      // read back results and verify they match
      validateRowCount(spark, filesystem, subdir, format, numRows)

      logInfo("Executing dynamic partitioned overwrite")

      val rows2 = 5
      val eventData2 = Events.events(year, year, 1, 1, rows2).toDS()
      val dynamicPartitioningToSucceed = expectDynamicPartitioningToSucceed(committerInfo)
      try {
        logDuration(s"overwrite datset2 to $subdir in format $format" +
          s"; expect success=$dynamicPartitioningToSucceed") {
          eventData2
            .write
            .mode("overwrite")
            .partitionBy("year", "month")
            .format(format)
            .save(subdir.toString)
        }
        assert(dynamicPartitioningToSucceed, "an operation requiring dynamic partitioning was called but did not fail")
        operations.verifyCommitter(subdir,
          Some(committerInfo),
          None,
          // Some(numPartitions),
          s"$format:")
      } catch {
        case e: IOException  => if (dynamicPartitioningToSucceed ||
          !e.getMessage.contains(DYNAMIC_PARTITION_UNSUPPORTED)) {
          throw e;
        }
          logInfo(s"Committer $committerName does not support dynamic binding")
      }
      anyOtherTests(spark, filesystem, subdir, format, sourceData, eventData2, committerInfo)

    } finally {
      spark.close()
    }

  }


  /**
   * Configure the context for dynamic partitioning
   * @param sparkConf spark conf
   * @param committerInfo committer in use
   */
  protected def setDynamicPartitioningOptions(
    sparkConf: SparkConf,
    committerInfo: CommitterInfo): Unit = {
    sparkConf.setAll(DYNAMIC_PARTITIONING)
  }

  protected def expectDynamicPartitioningToSucceed(
    committerInfo: CommitterInfo): Boolean = {
    dynamicPartitioning
  }

  /**
   * Override point.
   * Any other tests a subclass wants to execute after the first test dataset
   * has been created.
 *
   * @param spark context
   * @param filesystem dest fs
   * @param subdir subdir with dataset
   * @param format format of data
   * @param sourceData source DS
   * @param eventData2 second batch of events
   * @param committerInfo info about the committer
   */
  def anyOtherTests(spark: SparkSession, filesystem: FileSystem, subdir: Path,
    format: String, sourceData: Dataset[Event],
    eventData2: Dataset[Event],
    committerInfo: CommitterInfo): Unit = {

  }


}
