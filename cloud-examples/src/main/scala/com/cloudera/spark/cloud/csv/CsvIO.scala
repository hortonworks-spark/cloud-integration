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

package com.cloudera.spark.cloud.csv

import java.nio.charset.StandardCharsets
import java.util.zip.CRC32

import com.cloudera.spark.cloud.csv.CsvIO.{CsvReadOptions, CsvSchema, Permissive}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.statistics.IOStatisticsLogging._
import org.apache.hadoop.fs.statistics.IOStatisticsSupport._

import org.apache.spark.sql._
import org.apache.spark.sql.types.{LongType, StringType, StructType}


/**
 * Dataset class.
 * Latest build is "start","rowId","length","dataCrc","data","rowId2","end"
 */
case class CsvRecord(
    start: String,
    rowId: Long,
    length: Long,
    dataCrc: Long,
    data: String,
    rowId2: Long,
    end: String)

/**
 * Read, write and validate CsvRecords in different formats.
 *
 * @param spark   spark binding
 * @param conf    config file
 * @param records number of records to create/read
 */
class CsvIO(spark: SparkSession, conf: Configuration, records: Long) {
  // Ignore IDE warnings: this import is used

  import spark.implicits._

  /**
   * CRC of a string.
   *
   * @param s string
   * @return CRC32 value
   */
  def crc(s: String): Long = {
    val crc = new CRC32
    crc.update(s.getBytes(StandardCharsets.UTF_8))
    crc.getValue()
  }

  /**
   * Validate a record, returning a list which is non-empty if
   * an error occured.
   * @param r record
   * @return list of errors.
   */

  def validateRecord(r: CsvRecord): List[String] = {
    // scala is so purist about list mutability it's easier to use
    // an immutable type and mutable variable than a mutable linked list.
    var errors: List[String] = List()
    val rowId = r.rowId
    if (r.start != "start") {
      errors = s"invalid 'start' column" :: errors
    }
    if (rowId != r.rowId2) {
      errors = s"$rowId mismatch with tail rowid ${r.rowId2}" :: errors
    }
    val data = if (r.data != null) r.data else ""
    if (r.length != data.length) {
      errors =
        s"Invalid data length. Expected ${r.length} actual ${data.length}"  :: errors
    }
    val crcd = crc(data)
    if (r.dataCrc != crcd) {
      errors =
        s"Data checksum mismatch Expected ${r.dataCrc} actual ${crcd}." ::
          errors
    }
    if (r.end != "end") {
      errors = s"Invalid 'end' column" :: errors
    }
    errors

  }

  def validate(r: CsvRecord, verbose: Boolean): Unit = {
    if (verbose) {
      println(r)
    }
    val errors = validateRecord(r)

    if (errors.nonEmpty) {
      // trouble. log and then fail with a detailed message
      val message = new StringBuilder(s"Invalid row ${r.rowId} : $r. ")
      println(message)
      errors.foreach(println)
      errors.foreach(e => message.append(e).append("; "))
      throw new IllegalStateException(message.mkString)
    }
  }

  def path(s: String) = new Path(new java.net.URI(s))

  def p(s: String) = new Path(new java.net.URI(s))

  def p(p: Path, s: String) = new Path(p, s)

  def bind(p: Path): FileSystem = p.getFileSystem(conf)

  def ls(path: Path) = for (f <- bind(path).listStatus(path)) yield {f}


  /**
   * given a path string, get the FS and print its IOStats.
   */
  def iostats(s: String): String =
    ioStatisticsToPrettyString(retrieveIOStatistics(bind(path(s))))


  def validateDS(ds: Dataset[CsvRecord], verbose: Boolean = false) = {
    println(s"validating ${ds}")
    ds.foreach(r => validate(r, verbose))
    s"validation completed ${ds}"
  }

  def saveAs(ds: Dataset[CsvRecord], dest: String, format: String) = {
    println(s"Saving in format ${format} to ${dest}")
    ds.coalesce(1).
      write.
      mode("overwrite").
      format(format).
      save(dest)
    s"Saved in format ${format} to ${dest}"
  }

  def toAvro(ds: Dataset[CsvRecord], dest: String): Unit = {
    saveAs(ds, dest, "avro")
  }

  def toParquet(ds: Dataset[CsvRecord], dest: String): Unit = {
    saveAs(ds, dest, "parquet")
  }

  def toOrc(ds: Dataset[CsvRecord], dest: String): Unit = {
    saveAs(ds, dest, "orc")
  }

  def csvDataFrame(path: String, mode: String = Permissive): DataFrame =
    spark.read.options(CsvReadOptions).
      option("inferSchema", "false").
      option("mode", mode).
      schema(CsvSchema).
      csv(path)


  /**
   * Load a dataset.
   *
   * @param path    path
   * @param format  file format
   * @param options extra options
   * @return the dataset
   */
  def loadDS(path: String, format: String,
      options: Map[String, String] = Map()): Dataset[CsvRecord] =
    spark.read.
      schema(CsvSchema).
      format(format).
      options(options).
      load(path).
      as[CsvRecord]

}


object CsvIO {
  /**
   * The StructType of the CSV data.
   * "start","rowId","length","dataCrc","data","rowId2","rowCrc","end"
   */
  val CsvSchema: StructType = {
    new StructType().
      add("start", StringType). /* always "start" */
      add("rowId", LongType). /* row id when generated. */
      add("length", LongType). /* length of 'data' string */
      add("dataCrc", LongType). /* crc2 of the 'data' string */
      add("data", StringType). /* a char from [a-zA-Z0-9], repeated */
      add("rowId2", LongType). /* row id when generated. */
      add("end", StringType) /* always "end" */
  }

  // permissive parsing of records; the default
  val Permissive = "permissive";
  val FailFast = "failfast"
  val DropMalformed = "dropmalformed"

  val Start = "start";
  val End = "end";

  val CsvReadOptions: Map[String, String] = Map(
    "header" -> "true",
    "ignoreLeadingWhiteSpace" -> "false",
    "ignoreTrailingWhiteSpace" -> "false",
    //  "inferSchema" -> "false",
    "multiLine" -> "false")


  // Parquet options
  val ParquetValidateChecksums = "parquet.page.verify-checksum.enabled"

}
