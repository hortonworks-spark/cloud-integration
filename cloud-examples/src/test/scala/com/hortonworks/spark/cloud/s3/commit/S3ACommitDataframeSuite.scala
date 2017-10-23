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
import com.hortonworks.spark.cloud.utils.StatisticsTracker
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem

import org.apache.spark.sql.SparkSession

class S3ACommitDataframeSuite extends AbstractCommitterSuite {

  import com.hortonworks.spark.cloud.s3.S3ACommitterConstants._

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
    }
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
    DIRECTORY,
    //    PARTITIONED,
//    MAGIC,
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

  def testOneFormat(
      destDir: Path,
      format: String,
      committerName: Option[String]): Unit = {

    val local = getLocalFS
    val sparkConf = newSparkConf("DataFrames", local.getUri)
    val committerInfo = committerName.map(COMMITTERS_BY_NAME(_))

    committerInfo.foreach { info =>
      hconf(sparkConf, S3ACommitterConstants.S3A_SCHEME_COMMITTER_FACTORY, info._2)
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
      val numPartitions = 1
      val eventData = Events.events(2017, 2017, 1, 1, 10).toDS()
      val sourceData = eventData.repartition(numPartitions).cache()
      sourceData.printSchema()
      val eventCount = sourceData.count()
      logInfo(s"${eventCount} elements")
      sourceData.show(10)
      val numRows = eventCount

      val subdir = new Path(destDir, format)
      rm(s3, subdir)

      val stats = new StatisticsTracker(s3)

      logDuration(s"write to $subdir in format $format") {
        sourceData
          .write
          .partitionBy("year", "month")
          .format(format).save(subdir.toString)
      }
      val operations = new S3AOperations(s3)
      stats.update(s3)

      logDebug(s"Statistics = \n${stats.dump()}")

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
