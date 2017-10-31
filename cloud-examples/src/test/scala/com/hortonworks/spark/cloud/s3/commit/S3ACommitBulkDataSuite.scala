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
import org.apache.hadoop.fs.s3a.commit.files.SuccessData
import org.apache.hadoop.fs.{Path, PathExistsException}
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

  val paralellism = 6


  private val destFS = filesystemOption.orNull.asInstanceOf[S3AFileSystem]

  private val destDir = filesystemOption.map(f => testPath(f, "bulkdata")).orNull

  private var spark: SparkSession = _

  after {
    if (spark != null) {
      spark.close()
    }

    logInfo("Operation Summaries")
    summary.foreach(e =>
      logInfo(s"${e._1} ${toSeconds(e._2)}"))
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

  val operations = new S3AOperations(destFS)

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

    val committer = "partitioned"
    val sparkSession = newSparkSession(committer, "replace",
      Map(
        "spark.default.parallelism" -> paralellism.toString
      )
    )


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
    summarize("Build CSV RDD", tBuildRDD)
    logInfo("Add landsat columns")

    val csvDataFrame = LandsatIO.addLandsatColumns(rawCsvData)

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

    logInfo("Saving sampled and filtered ORC data")
    val (_, orcWriteTime) = writeDS(
      dest = landsatOrcPath,
      source = csvDataFrame.sample(false, 0.05d).filter("cloudCover < 75"),
      parted = false)

    logInfo(s"Write duration = ${toHuman(orcWriteTime)}")
    summarize("Filter and write orc unparted", orcWriteTime)


    //list destination files
    ls(landsatOrcPath, true).foreach { stat =>
      logInfo(s"  ${stat.getPath} + ${stat.getLen} bytes")
    }


    operations.maybeVerifyCommitter(landsatOrcPath,
      Some(committer),
      Some(COMMITTERS_BY_NAME(committer)),
      conf,
      None,
      Orc)

    // read in the unpartitioned
    val landsatOrcData = readLandsatDS(landsatOrcPath)

    // save it partitioned
    val landsatOrcByYearPath = new Path(fileMap(Orc), "parted")
    val (_, tOrcByYear) = writeDS(landsatOrcByYearPath, landsatOrcData, Orc, true)
    summarize("write orc parted", tOrcByYear)

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
    val (_, tCsvWrite) = writeDS(
      dest = landsat2014CSVParted,
      source = landsatOrcPartData.filter("year = 2014 AND cloudCover >= 0"),
      format = Csv,
      parted = true,
      extraOps = Map(CONFLICT_MODE -> CONFLICT_MODE_REPLACE))
    summarize("ORC -> csv where year = 2104 AND cloudCover >= 0", tCsvWrite)

    // bit of parquet, using same ops as ORC
    val landsatParquetPath = new Path(fileMap(Parquet), "parted-1")
    val (_, tParquetWrite) = writeDS(landsatParquetPath, landsatOrcData, Parquet, true )

    summarize("ORC -> Parquet", tParquetWrite)
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
    summarize("Parquet filter cloudcover < 0", tNegativeCloud2)

    // play with committer options.
    // first, write to directory with commit conflict = fail
    val landsatParquetPath2 = new Path(fileMap(Parquet), "parted-2")
    val (_, tParquetWrite2) = writeDS(
      dest = landsatParquetPath2,
      source = landsatOrcPartData.filter("year = 2013 AND cloudCover < 30"),
      format = Parquet,
      parted = true,
      extraOps = Map(
        S3A_COMMITTER_NAME -> DIRECTORY,
        CONFLICT_MODE -> CONFLICT_MODE_FAIL
      ))
    summarize("Parquet write 2013 directory+fail ", tParquetWrite2)

    val parquetDS2 = readLandsatDS(src = landsatParquetPath2, format = Parquet)

    val (parquetDS2_1, t_countDS2_1) = durationOf {
      parquetDS2.count()
    }

    summarize(s"Parquet read first count $parquetDS2_1", t_countDS2_1)

    // adding year = 2014 with fail MUST fail with directory committer as
    // base dir exists. Expect also: fail fast.

    val (_, tFailingDirCommit) = logDuration2("failing directory commit") {
      intercept[PathExistsException] {
        writeDS(
          dest = landsatParquetPath2,
          source = landsatOrcPartData.filter("year = 2014 AND cloudCover < 30"),
          format = Parquet,
          parted = true,
          extraOps = Map(
            S3A_COMMITTER_NAME -> DIRECTORY,
            CONFLICT_MODE -> CONFLICT_MODE_FAIL
          ))
      }
    }

    summarize("Failing parquet write 2014 directory+fail ", tFailingDirCommit)


    logInfo("Generated partitions")
    ls(landsatParquetPath2, true).foreach { stat =>
      if (stat.isDirectory) {
        logInfo(s"  ${stat.getPath}/")
      }
    }


    // now write parted +fail and expect all to be well, because
    // it is updating a different part from the 2013 data
    val (_, tPartUpdate) = writeDS(
      dest = landsatParquetPath2,
      source = landsatOrcPartData.filter("year = 2014 AND cloudCover < 30"),
      format = Parquet,
      parted = true,
      extraOps = Map(
        S3A_COMMITTER_NAME -> PARTITIONED,
        CONFLICT_MODE -> CONFLICT_MODE_FAIL
      ))
    summarize("Parquet write 2014 parts with conflict=fail ", tPartUpdate)

    // count
    val (parquetDS2_2, t_countDS2_2) = durationOf {
      readLandsatDS(src = landsatParquetPath2, format = Parquet).count()
    }
    summarize(s"Parquet read second count $parquetDS2_2", t_countDS2_2)

    assert(parquetDS2_2 > parquetDS2_1,
      s"Count of dataset $landsatParquetPath2 unchanged at $parquetDS2_1")

    // now append into the same dest dir with a different cloud year
    val (_, tPartAppend) = writeDS(
      dest = landsatParquetPath2,
      source = landsatOrcPartData.filter("" +
        "year = 2014 AND cloudCover > 50 AND cloudCover < 75"),
      format = Parquet,
      parted = true,
      extraOps = Map(
        S3A_COMMITTER_NAME -> PARTITIONED,
        CONFLICT_MODE -> CONFLICT_MODE_APPEND
      ))
    summarize("Parquet write append to existing partitions > parted + append",
      tPartAppend)

    val (parquetDS2_3, t_countDS2_3) = durationOf {
      readLandsatDS(src = landsatParquetPath2, format = Parquet).count()
    }
    summarize(s"Parquet read second count $parquetDS2_3", t_countDS2_3)


    // now do a failing part commit to same dest
    val (_, tFailingPartCommit) = logDuration2("failing directory commit") {
      intercept[PathExistsException] {
        writeDS(
          dest = landsatParquetPath2,
          source = landsatOrcPartData.filter("year = 2014 AND cloudCover >=75"),
          format = Parquet,
          parted = true,
          conflict = CONFLICT_MODE_FAIL)
      }
    }
    summarize("Failing Parquet write existing parts to fail", tFailingPartCommit)

    // get final stats
    destStats.update()

    logInfo(s"S3 Statistics diff ${destStats.dump()}")

  }

//  val codec = Some("lzo")
  val codec: Option[String] = None

  /**
    * Write a dataset
    * @param dest destination path
    * @param source source DS
    * @param format format
    * @param parted should the DS be parted by year & month?
    * @param committer name of committer
    * @param conflict conflict policy
    * @param extraOps extra operations to pass to the committer/context
    * @tparam T type of returned DS
    * @return success data
    */
  def writeDS[T](
      dest: Path,
      source: Dataset[T],
      format: String = Orc,
      parted: Boolean = false,
      committer: String = PARTITIONED,
      conflict: String = CONFLICT_MODE_FAIL,
      extraOps: Map[String, String] = Map()): (SuccessData, Long) = {

    logInfo(s"write to $dest in format $format partitioning: $parted")
    val t = time {
      val writer = source.write
      if(parted) {
        writer.partitionBy("year", "month")
      }
      writer.mode(SaveMode.Append)
      codec.foreach( writer.option("compression", _))
      extraOps.foreach(t => writer.option(t._1, t._2))
      writer.option(S3A_COMMITTER_NAME , committer)
      writer.option(CONFLICT_MODE, conflict)
      writer
        .format(format)
        .save(dest.toUri.toString)
    }
    logInfo(s"Write time: ${toSeconds(t)}")
    val success = operations.maybeVerifyCommitter(dest,
      Some(committer),
      Some(COMMITTERS_BY_NAME(committer)),
      destFS.getConf,
      None,
      s"Write to $dest in format $format")

    (success.get, t)
  }


}

