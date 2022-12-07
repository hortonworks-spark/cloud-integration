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

import java.net.URI

import com.cloudera.spark.cloud.common.SparkSessionCloudSuite
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class AbstractHugeCsvIOSuite extends SparkSessionCloudSuite {

  override def enabled: Boolean =
    super.enabled && isScaleTestEnabled

  val MiB = 10 * 1024 * 1024

  val DefaultRowCount = MiB

  /**
   * Method to give different row counts for different formats.
   *
   * @param format format
   * @return number of rows
   */
  def rowCount(format: String): Integer = {
    format match {
      case CsvIO.Parquet => 2 * DefaultRowCount
      case CsvIO.Orc => 2 * DefaultRowCount
      case _ => DefaultRowCount
    }
  }
  /**
   * Method to give different row counts for different formats.
   *
   * @param format format
   * @return number of rows
   */
  def validationAttempts(format: String): Integer = {
    format match {
      case CsvIO.Csv => 4
      case CsvIO.Avro => 4
      case _ => 1
    }
  }

  def createSparkSession(name: String = "session") = {
    SparkSession
      .builder
      .appName("DataFrames")
      .config(newSparkConf())
      .getOrCreate()
  }


  /**
   *
   * Create a Spark Session with the given keys.
   *
   * @param fsuri    filesystem uri
   * @param hive     enable hive support?
   * @param settings map of extra settings
   * @return the session
   */
  private def newSparkSession(
      fsuri: URI,
      hive: Boolean = false,
      settings: Traversable[(String, String)] = Map()): SparkSession = {

    val sparkConf = newSparkConf("session", fsuri)
    settings.foreach { case (k, v) => sparkConf.set(k, v) }

    val builder = SparkSession.builder
      .config(sparkConf)
    if (hive) {
      builder.enableHiveSupport
    }

    builder.getOrCreate()
  }

  ctest("validate-avro",
    "validate an avro dataset") {
    setSparkSession(newSparkSession(filesystem.getUri))

    val fmt = CsvIO.Avro
    val rows = rowCount(fmt)
    val csvio = new CsvIO(sparkSession, rows)
    val rowsRdd = csvio.generate(rows)
    //rowsRdd.foreach(r => CsvIO.validate(r, false))
    val rowsDF = sparkSession.createDataFrame(rowsRdd)

    val basePath = testPath(filesystem, "base")
    roundTrip(csvio, rowsDF, basePath, fmt, validationAttempts(fmt))
  }

  ctest("validate-csv",
    "validate a csv DS") {
    setSparkSession(newSparkSession(filesystem.getUri))
    val fmt = CsvIO.Csv
    val rows = rowCount(fmt)
    val csvio = new CsvIO(sparkSession, rows)
    val rowsRdd = csvio.generate(rows)
    val rowsDF = sparkSession.createDataFrame(rowsRdd)

    val basePath = testPath(filesystem, "base")
    roundTrip(csvio, rowsDF, basePath, fmt, validationAttempts(fmt))
  }

  /**
   * Round trip a format.
   *
   * @param csvio    csv io instance
   * @param source   source df
   * @param basePath base path for output
   * @param format   record format
   */
  def roundTrip(csvio: CsvIO, source: DataFrame, basePath: Path,
      format: String,
      iterations: Integer = 1,
      delete: Boolean = true): Unit = {
    println(s"output as $format to $basePath")
    filesystem.mkdirs(basePath)
    val table = new Path(basePath, format)
    filesystem.delete(table, true)


    try {
      logDuration(s"save as $format to $table") {
        source.coalesce(1).write
          .format(format)
          .save(table.toString)
      }

      lsR(table) {
        out => println(out)
      }

      val avroDS = logDuration(s"load as $format") {
        csvio.loadDS(table, format)
      }
      // this is where the read happens
      println(s"validating $table iterations=$iterations")
      for (i <- 1 to iterations) {
        logDuration(s"$i: validate $format DS") {
          csvio.validateDS(avroDS)
        }
      }

    } finally {
      logDuration("cleaning up") {
        filesystem.delete(table, true)
      }
    }

  }

  def lsR(path: Path)(out: String => Unit): Unit = {
    var size: Long = 0
    var count: Integer = 0
    val listing = filesystem.listFiles(path, true)
    while (listing.hasNext) {
      val st = listing.next()
      size += st.getLen
      count += 1
      out(s"${st.getPath} [${st.getLen}]")
    }
    out(s"summary $count files; total size=$size")
  }
}
