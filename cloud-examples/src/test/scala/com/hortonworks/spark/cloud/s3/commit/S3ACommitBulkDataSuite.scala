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

import scala.collection.mutable

import com.hortonworks.spark.cloud.examples.{LandsatIO, LandsatImage}
import com.hortonworks.spark.cloud.s3.{S3ACommitterConstants, S3AOperations, S3ATestSetup, SequentialIOPolicy}
import com.hortonworks.spark.cloud.utils.StatisticsTracker
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.{S3AFileSystem, S3AInputPolicy}

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

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

  override def enabled: Boolean = super.enabled &&
    hasCSVTestFile // && isScaleTestEnabled

  private val destFS = filesystemOption.orNull.asInstanceOf[S3AFileSystem]

  private val destDir = filesystemOption.map(f => testPath(f, "bulkdata"))
    .orNull

  private var spark: SparkSession = _

  after {
    if (spark != null) {
      spark.close()
    }
  }

  protected val Parquet = "parquet"
  protected val Csv = "csv"
  protected val Orc = "orc"

  private val formats = Seq(
    Orc,
    Parquet,
    Csv,
    ""
  )

  /**
   * Test one write sequence
   *
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
    // to decide how to react to individual partitions, rather than
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
      Some(committerInfo),
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
      Some(committerInfo),
      conf,
      Some(newFileCount),
      s"$format:")

    // now list the files under the system
    val allFiles = listFiles(destFS, dest, true).filterNot(
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
   *
   * @param committerName name of the committer
   * @param confictMode conflict mode to use
   * @return the session
   */
  private def newSparkSession(
      committerName: String, confictMode: String,
      settings: Traversable[(String, String)] = Map()): SparkSession = {
    val local = getLocalFS

    val sparkConf = newSparkConf("DataFrames", local.getUri)
    sparkConf.setAll(settings)
    val committerInfo = COMMITTERS_BY_NAME(committerName)
    committerInfo.bind(sparkConf)

    logInfo(s"Using committer $committerInfo with conflict mode $confictMode")
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

  val paralellism = 4

  /**
   * This is the test
   */



  ctest("landsat pipeine", "work with the landsat data") {
    val csvPath = getTestCSVPath()

    val committer = "partitioned"
    val sparkSession = newSparkSession(committer, "replace",
      Map(
        "spark.default.parallelism" -> paralellism.toString
      )
    )

    // summary of operations
    val summary = new mutable.ListBuffer[(String, Long)]()

    spark = sparkSession
    val conf = spark.sparkContext.hadoopConfiguration

    val destPath = new Path(destDir, "output")
    rm(destFS, destPath)
    val landsatPath = new Path(destPath, "landsat")

    val fileMap = nonEmpty(formats).
      map(fmt => fmt -> new Path(landsatPath, fmt)).toMap

    val landsatOrcPath = new Path(fileMap("orc"), "filtered")

    // make very sure that the FS is normal IO
    val csvFS = csvPath.getFileSystem(conf).asInstanceOf[S3AFileSystem]
    assert(csvFS.getInputPolicy === S3AInputPolicy.Sequential,
      s"wrong input policy for $csvPath in $csvFS")

    // ignore the IDE if it complains: this *is* used.
    import sparkSession.implicits._

    val csvSchema = LandsatIO.buildCsvSchema()
    logInfo("CSV Schema:")
    csvSchema.printTreeString()
    val (rawCsvData, tBuildRDD) = logDuration2("set up initial .csv load") {
      spark.read.options(LandsatIO.CsvOptions)
        .schema(csvSchema)
        .csv(csvPath.toUri.toString)
    }
    summary += (("Build CSV RDD", tBuildRDD))
    logInfo("Add landsat columns")

    val csvDataFrame = LandsatIO.addLandsatColumns(rawCsvData)

    def readLandsatDS(
        src: Path,
        format: String = Orc): Dataset[LandsatImage] = {

      val op = s"read $src as $format"
      val (r, t) = logDuration2(op) {
        spark.read.format(format).load(src.toUri.toString).as[LandsatImage]
      }
      summary += ((op, t))
      r
    }

    val destStats = new StatisticsTracker(destFS)

    logInfo("Saving")
    val (_, orcWriteTime) = writeDS(
      dest = landsatOrcPath,
      source = csvDataFrame.sample(false, 0.05d).filter("cloudCover < 30"))

    logInfo(s"Write duration = ${toHuman(orcWriteTime)}")
    summary += (("Filter and write orc unparted", orcWriteTime))


    //list destination files
    ls(landsatOrcPath, true).foreach { stat =>
      logInfo(s"  ${stat.getPath} + ${stat.getLen} bytes")
    }


    val operations = new S3AOperations(destFS)
    operations.maybeVerifyCommitter(landsatOrcPath,
      Some(committer),
      Some(COMMITTERS_BY_NAME(committer)),
      conf,
      None,
      Orc)

    // now do some dataframe
    val landsatOrcData = readLandsatDS(landsatOrcPath)

    val landsatOrcByYearPath = new Path(fileMap(Orc), "parted")
    val (_, tOrcByYear) = writeDS(landsatOrcByYearPath, landsatOrcData, Orc, true)
    summary += (("write orc parted", tOrcByYear))
    val landsatPartData = readLandsatDS(landsatOrcByYearPath)
    val (nveCloudCover, tNegativeCloud) =
      logDuration2(s"Filter on year and cloudcover of $landsatOrcByYearPath") {
      val negative = landsatPartData
        .filter("year = 2013 AND cloudCover < 0")
        .sort("year")
      negative.show(10)
      negative.count()
    }
    logInfo(s"Number of entries with negative cloud cover: $nveCloudCover")
    summary += (("ORC filter cloudcover < 0", tNegativeCloud))

    val landsat2014CSVParted = new Path(fileMap(Csv), "parted/y2013")
    val (_, tCsvWrite) = writeDS(
      dest = landsat2014CSVParted,
      source = landsatPartData.filter("year = 2014 AND cloudCover >= 0"),
      format = Csv,
      parted = true,
      extraOps = Map("fs.s3a.staging.conflict" -> "replace"))
    summary += (("ORC -> csv where year = 2104 AND cloudCover >= 0", tCsvWrite))

    // bit of parquet, using same ops as ORC
    val landsatParquetPath = new Path(fileMap(Parquet), "parted")
    val (_, tParquetWrite) = writeDS(landsatParquetPath, landsatOrcData, Parquet, true )

    summary += (("ORC -> Parquet", tParquetWrite))
    operations.maybeVerifyCommitter(landsatParquetPath,
      Some(committer),
      Some(COMMITTERS_BY_NAME(committer)),
      conf,
      None,
      Parquet)

    val landsatParquetData = readLandsatDS(landsatParquetPath, Parquet)
    val (nveCloudCover2, tNegativeCloud2) =
      logDuration2(s"Filter on year and cloudcover of $landsatOrcByYearPath") {
        val negative = landsatParquetData
          .filter("year = 2013 AND cloudCover < 0")
          .sort("year")
        negative.show(10)
        negative.count()
      }
    assert(nveCloudCover === nveCloudCover2, "cloud cover queries across formats")
    summary += (("Parquet filter cloudcover < 0", tNegativeCloud2))


    destStats.update()

    logInfo(s"S3 Statistics diff ${destStats.dump()}")

    logInfo("Operation Summaries")
    summary.foreach(e =>
      logInfo(s"${e._1} ${toSeconds(e._2)}"))
  }

//  val codec = Some("lzo")
  val codec: Option[String] = None

  def writeDS[T](
      dest: Path,
      source: Dataset[T],
      format: String = Orc,
      parted: Boolean = false,
      extraOps: Map[String, String] = Map()): (Dataset[T], Long) = {

    logInfo(s"write to $dest in format $format partitioning: $parted")
    val t = time {
      val writer = source.write
      if(parted) {
        writer.partitionBy("year", "month")
      }
      writer.mode(SaveMode.Append)
      codec.foreach( writer.option("compression", _))
      extraOps.foreach(t => writer.option(t._1, t._2))
      writer
        .format(format)
        .save(dest.toUri.toString)
    }
    logInfo(s"Write time: ${toSeconds(t)}")
    (source, t)
  }


}

