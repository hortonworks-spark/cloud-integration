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

import com.cloudera.spark.cloud.local.LocalTestSetup
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class LocalHugeCsvIOSuite extends AbstractHugeCsvIOSuite with LocalTestSetup {

  init()

  /**
   * set up FS if enabled.
   */
  def init(): Unit = {
    initFS()
  }

  def rowCount = 1024 * 1024


  def createSparkSession(name: String ="session") = {
    SparkSession
      .builder
      .appName("DataFrames")
      .config(newSparkConf())
      .getOrCreate()
  }


  /**
   *
   * Create a Spark Session with the given keys.
   * @param fsuri filesystem uri
   * @param hive enable hive support?
   * @param settings map of extra settings
   * @return the session
   */
  private def newSparkSession(
      fsuri: URI,
      hive: Boolean = false,
      settings: Traversable[(String, String)] = Map()): SparkSession = {
    val local = getLocalFS

    val sparkConf = newSparkConf("session", fsuri)
    settings.foreach { case (k, v) => sparkConf.set(k, v) }

    val builder = SparkSession.builder
      .config(sparkConf)
    if (hive) {
      builder.enableHiveSupport
    }
    builder.getOrCreate()
  }

  ctest("csv-io",
    "test CSV IO") {
    setSparkSession(newSparkSession(filesystem.getUri))

    val csvio = new CsvIO(sparkSession, rowCount)
    val rowsRdd = csvio.generate(rowCount)
    rowsRdd.foreach(r => CsvIO.validate(r, false))
    val rowsDF = sparkSession.createDataFrame(rowsRdd)

    val basePath = testPath(filesystem, "base")
    filesystem.mkdirs(basePath)
    roundTrip(csvio, rowsDF,basePath, "avro")
  }

  def roundTrip(csvio: CsvIO, source: DataFrame, basePath: Path, format: String) = {
    val avroPath = new Path(basePath, format)


    logDuration("save as " + format) {
      source.coalesce(1).write
        .format(format)
        .save(avroPath.toString)
    }

    val avroDS = logDuration("load as " + format) {
      csvio.loadDS(avroPath, "avro")
    }
    csvio.validateDS(avroDS)
    assert(source.count() == avroDS.count())


  }
}
