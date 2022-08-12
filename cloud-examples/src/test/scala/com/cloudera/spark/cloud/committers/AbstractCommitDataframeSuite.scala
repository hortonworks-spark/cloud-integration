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

import com.cloudera.spark.cloud.CommitterBinding._
import com.cloudera.spark.cloud.s3.S3AOperations
import com.cloudera.spark.cloud.s3.commit.Events
import com.cloudera.spark.cloud.utils.StatisticsTracker
import com.cloudera.spark.cloud.ObjectStoreConfigurations.DYNAMIC_PARTITIONING
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem

import org.apache.spark.sql.SparkSession

/**
 * Tests different data formats through the committers.
 */
abstract class AbstractCommitDataframeSuite extends AbstractCommitterSuite {


  /**
   * Formats to test.
   */
  private val formats = Seq(
    "orc",
    "parquet",
    ""
  )

  // there's an empty string at the end to aid with commenting out different
  // committers and not have to worry about any trailing commas
  def committers(): Seq[String]

  def dynamicPartitioning: Boolean

  def schema: String

  private lazy val destDir = testPath(filesystem, "dataframe-committer")

  nonEmpty(committers).foreach { committer =>
    nonEmpty(formats).foreach {
      fmt =>
        val commitInfo = COMMITTERS_BY_NAME(committer)
        ctest(s"Dataframe+$committer-$fmt",
          s"Write a dataframe to $fmt with the committer $committer") {
          testOneFormat(
            new Path(destDir, s"committer-$committer-$fmt"),
            fmt,
            committer)
        }
    }
  }

  def testOneFormat(
      destDir: Path,
      format: String,
      committerName: String): Unit = {

    val local = getLocalFS
    val sparkConf = newSparkConf("DataFrames", local.getUri)
    val committerInfo = COMMITTERS_BY_NAME(committerName)


    committerInfo.bindToSchema(sparkConf, schema)

    if (dynamicPartitioning) {
      sparkConf.setAll(DYNAMIC_PARTITIONING)
    }

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
      val numPartitions = 1
      val eventData = Events.events(2017, 2017, 1, 1, 10).toDS()
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
      val operations = new S3AOperations(filesystem)
      stats.update()

      logDebug(s"Statistics = \n${stats.dump()}")

      operations.maybeVerifyCommitter(subdir,
        Some(committerName),
        Some(committerInfo),
        conf,
        Some(numPartitions),
        s"$format:")
      // read back results and verify they match
      validateRowCount(spark, filesystem, subdir, format, numRows)
    } finally {
      spark.close()
    }

  }

}
