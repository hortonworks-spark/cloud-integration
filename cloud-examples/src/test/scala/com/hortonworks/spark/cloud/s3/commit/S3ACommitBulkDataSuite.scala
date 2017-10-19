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

import com.hortonworks.spark.cloud.commit.CommitterConstants
import com.hortonworks.spark.cloud.examples.{LandsatIO, LandsatImage}
import com.hortonworks.spark.cloud.s3.{S3ACommitterConstants, S3AOperations, S3ATestSetup, SequentialIOPolicy}
import com.hortonworks.spark.cloud.utils.StatisticsTracker
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.s3a.{S3AFileSystem, S3AInputPolicy}

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * This is a large data workflow, starting with the landsat
 * dataset
 */
class S3ACommitBulkDataSuite extends AbstractCommitterSuite with S3ATestSetup
  with SequentialIOPolicy {

  import com.hortonworks.spark.cloud.s3.S3ACommitterConstants._

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
      prepareTestCSVFile()
    }
  }

  override def enabled: Boolean = super.enabled && hasCSVTestFile // && isScaleTestEnabled

  private val destFS = filesystemOption.orNull.asInstanceOf[S3AFileSystem]

  private val destDir = filesystemOption.map(f =>  testPath(f, "bulkdata")).orNull

  private var spark: SparkSession = _

  after {
    if (spark != null) {
      spark.close()
    }
  }

  private val formats = Seq(
    "orc",
    "parquet"
  )



  private val modes = Seq("append", "replace")


/*
  formats.foreach { format =>
    modes.foreach{ mode =>
      ctest(s"Write $format-$mode",
        s"Write a partitioned dataframe in format $format with conflict mode $mode",
        false) {
        testOneWriteSequence(
          new Path(destDir, s"$format/$mode"),
          format,
          PARTITIONED,
          mode,
          true)
      }

    }
  }*/

  /**
   * Test one write sequence
   * @param destDir destination
   * @param format output format
   * @param committerName committer name to use
   * @param confictMode how to deal with conflict
   * @param expectAppend should the output be expected to be appended or overwritten
   */
  def testSequence(
      destDir: Path,
      format: String,
      committerName: String,
      confictMode: String,
      expectAppend: Boolean): Unit = {

    val committerInfo = COMMITTERS_BY_NAME(committerName)

    val dest = new Path(destDir, format)
    rm(destFS, dest)
    val spark = newSparkSession(committerName, confictMode)

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


  }

  /**
   *
   * Create a Spark Session with the given committer setup
   * @param committerName name of the committer
   * @param confictMode conflict mode to use
   * @return the session
   */
  private def newSparkSession(committerName: String, confictMode: String,
      settings: Traversable[(String, String)] = Map()): SparkSession = {
    val local = getLocalFS

    val sparkConf = newSparkConf("DataFrames", local.getUri)
    sparkConf.setAll(settings)
    val committerInfo = COMMITTERS_BY_NAME(committerName)

    val factory = committerInfo._2
    hconf(sparkConf, S3ACommitterConstants.S3A_COMMITTER_FACTORY_KEY, factory)
    logInfo(s"Using committer factory $factory with conflict mode $confictMode" )
    hconf(sparkConf, S3ACommitterConstants.CONFLICT_MODE, confictMode)

    // landsat always uses normal IO
    hconf(sparkConf,
      "fs.s3a.bucket.landsat-pds.experimental.fadvise",
      S3AInputPolicy.Sequential.toString)


    // force failfast

    SparkSession.builder
      .config(sparkConf)
      .enableHiveSupport
      .getOrCreate()
  }

  val csvOptions = Map(
    "header" -> "true",
    "ignoreLeadingWhiteSpace" -> "true",
    "ignoreTrailingWhiteSpace" -> "true",
    "timestampFormat" -> "dd-MM-yyyy HH:mm",
    "inferSchema" -> "false",
    "mode" -> "DROPMALFORMED")

  val paralellism = 1

  /**
   * This is the test
   */
  ctest("landsat pipeine", "work with the landsat data") {
    val csvPath = getTestCSVPath()

    val committerName = "directory"
    val sparkSession = newSparkSession(committerName, "append",
      Map(
        "spark.default.parallelism" -> paralellism.toString
      )
    )

    spark = sparkSession
    val conf = spark.sparkContext.hadoopConfiguration

    // make very sure that the FS is normal IO
    val csvFS = csvPath.getFileSystem(conf).asInstanceOf[S3AFileSystem]
    assert(csvFS.getInputPolicy === S3AInputPolicy.Sequential,
      s"wrong input policy for $csvPath in $csvFS")


    // ignore the IDE if it complains: this *is* used.
    import sparkSession.implicits._

    val rawCsvData = spark.read.options(LandsatIO.csvOptions)
      .csv(csvPath.toUri.toString)
    val csvDataFrame = LandsatIO.addLandsatColumns(rawCsvData)

    val localSnapshotDir = tempDir("landsat", "")
    val localSnapshotPath = new Path(localSnapshotDir.toURI)
    writeDataframe(localSnapshotPath,
      csvDataFrame,
      "orc",
      "replace")

    val localFiles = getLocalFS.listStatus(localSnapshotPath)
    assert(localFiles.nonEmpty, "No local files written")

    val localData = spark.read.orc(localSnapshotPath.toUri.toString)

    val filteredData = logDuration(s"Filter and cache the CSV source $csvPath") {
      localData.filter("cloudCover < 30")
    }
    logInfo(s"Record count ${filteredData.count()}")

    val destPath = new Path(destDir, "output")
    rm(destFS, destPath)
    val landsatPath = new Path(destPath, "landsat")
    val landsatParqetPath = new Path(landsatPath, "parquet")

    val formats = Seq("orc","parquet")


    val fileMap: Map[String, Path] = formats.
      map(fmt => fmt -> new Path(landsatPath, fmt)).toMap

    val landsatOrcPath = fileMap("orc")

    val stats = new StatisticsTracker(destFS)
    val orcWrite = writeDataframe(landsatOrcPath, filteredData, "orc", "replace")
    stats.update(destFS)

    logInfo("Write duration = %3.3f".format((orcWrite/1000.0f)))
    logInfo(s"Statistics diff ${stats.dump()}")

    ls(landsatOrcPath, true).foreach { stat =>
      logInfo(s"  ${stat.getPath} + ${stat.getLen} bytes")
     }

    //list destination files
    val operations = new S3AOperations(destFS)
    val numPartitions = 1
    operations.maybeVerifyCommitter(landsatOrcPath,
      Some(committerName),
      Some(COMMITTERS_BY_NAME(committerName)._1),
      conf,
      Some(numPartitions),
      "ORC")

/*

    val summary = fileMap.map { case (fmt, path) =>
      fmt -> (path, writeDataframe(path, csvDataFrame, fmt, "replace"))
    }.toMap
*/
  }

  // Write the DS. Configure save mode so the committer gets
  // to decide how to react to invidual partitions, rather than
  // have the entire directory tree determine the outcome.
  def writeDataset(dest: Path, sourceData: Dataset[LandsatImage], format: String,
      confictMode: String): Long = {


    logInfo(s"write to $dest in format $format conflict = $confictMode")
    time {
      sourceData
        .write
//        .partitionBy("year", "month")
        .mode(SaveMode.Append)
        .option("compression", "snappy")
        .format(format).save(dest.toUri.toString)
    }
  }

  def writeDataframe(
      dest: Path, sourceData: DataFrame, format: String,
      confictMode: String): Long = {

    logInfo(s"write to $dest in format $format conflict = $confictMode")
    val t = time {
      sourceData
        .write
        //        .partitionBy("year", "month")
        .mode(SaveMode.Append)
        .option("compression", "snappy")
        .format(format).save(dest.toUri.toString)
    }
    logInfo(s"Write time: ${toHuman(t)}")
    t
  }


}

