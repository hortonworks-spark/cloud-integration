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

package com.cloudera.spark.cloud.gs

import scala.collection.mutable

import com.cloudera.spark.cloud.s3.{LandsatImage, LandsatIO, S3AOperations}
import com.cloudera.spark.cloud.s3.S3ACommitterConstants._
import com.cloudera.spark.cloud.utils.StatisticsTracker
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.{S3AFileSystem, S3AInputPolicy}
import org.apache.hadoop.fs.s3a.commit.files.SuccessData

import org.apache.spark.sql.{Dataset, SparkSession}
/**
 * This is a large data workflow, starting with the landsat
 * dataset
 */
class GsCommitBulkDataSuite extends AbstractGsCommitterSuite
  {

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

  private val parallelism = 8

  private val destFS = filesystem

  private val destDir = filesystemOption.map(f => testPath(f, "bulkdata")).orNull

  private var spark: SparkSession = _

  after {
    if (spark != null) {
      spark.close()
    }

    val s = summary.map(e => e._1 + ": " + toSeconds(e._2)).mkString("\n")
    logInfo(s"Operation Summaries\n$s")
  }

  private val formats = Seq(
    Orc,
    Parquet,
    Csv,
    ""
  )

  // choose the filter percentage; on a scale test
  // the output is bigger
  val filterPercentage: Double = if (isScaleTestEnabled) 0.80d else 0.10d

  val operations = new S3AOperations(destFS)

  /**
   *
   * Create a Spark Session with the given committer setup
   *
   * @param committerName name of the committer
   * @param confictMode conflict mode to use
   * @return the session
   */
  private def newSparkSession(settings: Traversable[(String, String)] = Map()) = {
    val local = getLocalFS

    val sparkConf = newSparkConf("DataFrames", local.getUri)
    settings.foreach { case (k, v) => sparkConf.set(k, v) }

    // landsat always uses sequential
    hconf(sparkConf,
      "fs.s3a.bucket.landsat-pds.experimental.input.fadvise",
      S3AInputPolicy.Sequential.toString)


    // force failfast

    SparkSession.builder
      .config(sparkConf)
      .enableHiveSupport
      .getOrCreate()
  }


  /**
   * This is the test
   */

  // summary of operations
  val summary = new mutable.ListBuffer[(String, Long)]()

  private def summarize(s: String, t: Long) = {
    logInfo(s"Duration of $s = ${toSeconds(t)}")
    summary += ((s, t))
  }



  ctest("landsat pipeine", "work with the landsat data") {
    val csvPath = getTestCSVPath()

    val committer = "manifest"
    val sparkSession = newSparkSession(Map(
                "spark.default.parallelism" -> parallelism.toString))


    spark = sparkSession
    val conf = spark.sparkContext.hadoopConfiguration

    val destPath = new Path(destDir, "output")
    rm(destFS, destPath)
    val landsatPath = new Path(destPath, "landsat")

    val fileMap = nonEmpty(formats).
      map(fmt => fmt -> new Path(landsatPath, fmt)).toMap

    val landsatOrcPath = new Path(fileMap("orc"), "filtered")

    csvPath.getFileSystem(conf) match {
      case csvFS: S3AFileSystem =>
        val inputPolicy = csvFS.getInputPolicy
        assert(inputPolicy === S3AInputPolicy.Normal
          || inputPolicy === S3AInputPolicy.Sequential,
          s"wrong input policy for $csvPath in $csvFS")
      case _ =>
    }
    // ignore the IDE if it complains: this *is* used.
    import sparkSession.implicits._

    val csvSchema = LandsatIO.buildCsvSchema()
    logInfo(s"CSV Schema: of $csvPath")
    csvSchema.printTreeString()
    val (rawCsvData, tBuildRDD) = logDuration2(s"Initial .csv read of $csvPath") {
      spark.read.options(LandsatIO.CsvOptions)
        .schema(csvSchema)
        .csv(csvPath.toUri.toString)
    }
    summarize("Build CSV RDD", tBuildRDD)
    logInfo("Add landsat columns")

    val csvDataFrame = LandsatIO.addLandsatColumns(rawCsvData)

    /**
     * Read data to landsat image entries.
     * @param src source path
     * @param format source format
     * @return the dataset
     */
    def readLandsatDS(
        src: Path,
        format: String = Orc): Dataset[LandsatImage] = {

      val op = s"read $src as $format"
      val (r, t) = logDuration2(op) {
        spark.read.format(format).load(src.toUri.toString).as[LandsatImage]
      }
      summarize(op, t)
      r
    }

    val destStats = new StatisticsTracker(destFS)

    val filteredCSVSource = csvDataFrame.sample(false, filterPercentage)
      .filter("cloudCover < 75")
    writeDS(
      summary = "sample and filter landsat CSV",
      dest = landsatOrcPath,
      source = filteredCSVSource,
      parted = false)


    // read in the unpartitioned
    val landsatOrcData = readLandsatDS(landsatOrcPath)

    // save it partitioned
    val landsatOrcByYearPath = new Path(fileMap(Orc), "parted")
    writeDS(
      summary = "write orc parted",
      dest = landsatOrcByYearPath,
      source = landsatOrcData,
      format = Orc)

    // read in the partitioned data; filter and count
    val landsatOrcPartData = readLandsatDS(landsatOrcByYearPath)
    val (nveCloudCover, tNegativeCloud) =
      logDuration2(s"Filter on year and cloudcover of $landsatOrcByYearPath") {
      val negative = landsatOrcPartData
        .filter("year = 2013 AND cloudCover < 0")
        .sort("year")
      negative.show(10)
      negative.count()
    }
    logInfo(s"Number of entries with negative cloud cover: $nveCloudCover")
    summarize("ORC filter cloudcover < 0", tNegativeCloud)

    // generate CSV for 2014 and positive clouds
    val landsat2014CSVParted = new Path(fileMap(Csv), "parted")
    writeDS(
      summary = "ORC -> csv where year = 2014 AND cloudCover >= 0",
      dest = landsat2014CSVParted,
      source = landsatOrcPartData.filter("year = 2014 AND cloudCover >= 0"),
      format = Csv,
      conflict = CONFLICT_MODE_REPLACE)

    // bit of parquet, using same ops as ORC
    val landsatParquetPath = new Path(fileMap(Parquet), "parted-1")
    writeDS(
      summary = "ORC -> Parquet",
      dest = landsatParquetPath,
      source = landsatOrcData,
      format = Parquet)

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
    summarize("Parquet filter cloudcover < 0", tNegativeCloud2)

    // play with committer options.
    // first, write to directory with commit conflict = fail
    val landsatParquetPath2 = new Path(fileMap(Parquet), "parted-2")
    val outcome = writeDS(
      summary = "Parquet write 2013 data",
      dest = landsatParquetPath2,
      source = landsatOrcPartData.filter("year = 2013 AND cloudCover < 30"),
      format = Parquet)

    val parquetDS2 = readLandsatDS(src = landsatParquetPath2, format = Parquet)

    val (parquetDS2_1, t_countDS2_1) = durationOf {
      parquetDS2.count()
    }

    summarize(s"Parquet read first count $parquetDS2_1", t_countDS2_1)

    // adding year = 2014 with fail MUST fail with directory committer as
    // base dir exists. Expect also: fail fast.
/*

    logInfo("Expect a stack trace")
    logInfo("====================")

    val (_, tFailingDirCommit) = logDuration2("failing directory commit") {
      intercept[PathExistsException] {
        writeDS(
          summary = "failing directory commit",
          dest = landsatParquetPath2,
          source = landsatOrcPartData.filter("year = 2014 AND cloudCover < 30"),
          format = Parquet)
      }
    }

    summarize("Failing parquet write 2014 directory+fail ", tFailingDirCommit)
    logInfo("====================")

    logInfo("Generated partitions")
    ls(landsatParquetPath2, true).foreach { stat =>
      if (stat.isDirectory) {
        logInfo(s"  ${stat.getPath}/")
      }
    }

*/

    // now write parted +fail and expect all to be well, because
    // it is updating a different part from the 2013 data
    writeDS(
      summary = "Parquet write 2014 & cloud > 30",
      dest = landsatParquetPath2,
      source = landsatOrcPartData.filter("year = 2014 AND cloudCover < 30"),
      format = Parquet)

    // count
    val (parquetDS2_2, t_countDS2_2) = durationOf {
      readLandsatDS(src = landsatParquetPath2, format = Parquet).count()
    }
    summarize(s"Parquet read second count $parquetDS2_2", t_countDS2_2)

    assert(parquetDS2_2 > parquetDS2_1,
      s"Count of dataset $landsatParquetPath2 unchanged at $parquetDS2_1")

    // now append into the same dest dir with a different year
    writeDS(
      summary = "Append to existing parts",
      dest = landsatParquetPath2,
      source = landsatOrcPartData.filter("" +
        "year = 2014 AND cloudCover > 50 AND cloudCover < 75"),
      format = Parquet,
      conflict = CONFLICT_MODE_APPEND
    )

    val (parquetDS2_3, t_countDS2_3) = durationOf {
      readLandsatDS(src = landsatParquetPath2, format = Parquet).count()
    }
    summarize(s"Parquet read third count $parquetDS2_3", t_countDS2_3)

    assert(t_countDS2_3 > t_countDS2_2,
      s"Count of dataset $landsatParquetPath2 unchanged at $parquetDS2_2")


    // now do a failing part commit to same dest
/*
    logWarning("Ignored stack traces in the next section")
    logWarning("========================================")
    val (_, tFailingPartCommit) = logDuration2("failing part commit") {
      intercept[SparkException] {
        val outcome = writeDS(
          summary = "failing part commit",
          dest = landsatParquetPath2,
          source = landsatOrcPartData.filter("year = 2013"),
          format = Parquet,
          conflict = CONFLICT_MODE_FAIL)
        logWarning(s"Success outcome: ${outcome.success.toString}")
      }
    }
    logWarning("========================================")
    summarize("Failing Parquet write existing parts to fail", tFailingPartCommit)
*/

    // before asserting that a failing part commit where there is no output
    // is not an error, because there are no parts to conflict
/*
    val r = writeDS(
      summary = "Append to existing partitions with empty output",
      dest = landsatParquetPath2,
      source = landsatOrcPartData.filter("year = 2014 AND cloudCover > 150"),
      format = Parquet,
      conflict = CONFLICT_MODE_FAIL)
    assert(r.success.getFilenames.isEmpty, s"Expected no files in ${r.success}")

*/
    // get stats
    destStats.update()

    logInfo(s"Statistics diff ${destStats.dump()}")

  }

  /**
    * Write a dataset
    * @param dest destination path
    * @param source source DS
    * @param format format
    * @param parted should the DS be parted by year & month?
    * @param conflict conflict policy
    * @param extraOps extra operations to pass to the committer/context
    * @tparam T type of returned DS
    * @return success data
    */
  def writeDS[T](
    @transient dest: Path,
    source: Dataset[T],
    summary: String = "",
    format: String = Orc,
    parted: Boolean = true,
    conflict: String = CONFLICT_MODE_FAIL,
    extraOps: Map[String, String] = Map()): WriteOutcome[T] = {

    val committer: String = MANIFEST

    val t = writeDataset(
      destFS,
      dest,
      source,
      summary,
      format,
      parted,
      committer,
      conflict,
      extraOps)
    val text = s"$summary + format $format partitioning: $parted" +
      s" conflict=$conflict"
    summarize(summary, t)
    val success = operations.maybeVerifyCommitter(dest,
      Some(committer),
      Some(COMMITTERS_BY_NAME(committer)),
      destFS.getConf,
      None,
      text)
    WriteOutcome(source, dest, success.get, t)
  }

  case class WriteOutcome[T](
    source: Dataset[T],
    dest: Path,
    success: SuccessData,
    duration: Long)

}

