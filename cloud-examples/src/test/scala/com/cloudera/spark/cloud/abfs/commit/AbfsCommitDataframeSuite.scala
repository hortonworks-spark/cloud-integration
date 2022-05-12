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

package com.cloudera.spark.cloud.abfs.commit

import com.cloudera.spark.cloud.s3.S3AOperations
import com.cloudera.spark.cloud.s3.commit.Events
import com.cloudera.spark.cloud.utils.StatisticsTracker
import com.cloudera.spark.cloud.GeneralCommitterConstants.{ABFS_MANIFEST_COMMITTER_FACTORY, ABFS_SCHEME_COMMITTER_FACTORY, MANIFEST_COMMITTER_NAME}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession

class AbfsCommitDataframeSuite extends AbstractAbfsCommitterSuite {

  init()

  def init(): Unit = {
    if (enabled) {
      initFS()
    }
  }

  /**
   * Formats to test.
   */
  private val formats = Seq(
    "orc",
    "parquet",
    ""
  )

  private val committers = Seq("manifest")
  private lazy val destDir = testPath(filesystem, "dataframe-committer")

  nonEmpty(formats).foreach {
    fmt =>
      ctest(s"Dataframe+manifest-$fmt",
        s"Write a dataframe to $fmt with the manifest committer") {
        testOneFormat(
          new Path(destDir, s"manifest-$fmt"),
          fmt)
      }
  }

  def testOneFormat(
      destDir: Path,
      format: String): Unit = {

    val committerName =Some(MANIFEST_COMMITTER_NAME);
    val local = getLocalFS
    val sparkConf = newSparkConf("DataFrames", local.getUri)


    // TODO make option if we want to support regression testing with the old
    // committer
    hconf(sparkConf,
      ABFS_SCHEME_COMMITTER_FACTORY, ABFS_MANIFEST_COMMITTER_FACTORY)



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
      rm(filesystem, subdir)

      val stats = new StatisticsTracker(filesystem)

      logDuration(s"write to $subdir in format $format") {
        sourceData
          .write
          .partitionBy("year", "month")
          .format(format).save(subdir.toString)
      }
      val operations = new S3AOperations(filesystem)
      stats.update()

      logDebug(s"Statistics = \n${stats.dump()}")

      operations.maybeVerifyCommitter(subdir,
        committerName,
        None,
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
