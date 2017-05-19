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

package com.hortonworks.spark.cloud.s3

import com.hortonworks.spark.cloud.CloudSuite
import org.apache.hadoop.fs._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * A suite of tests working with boris bike data from Transport for London.
 * If you don't have the
 */
class BorisBikeSuite extends CloudSuite with S3ATestSetup {

  /**
   * Minimum number of lines, from `gunzip` + `wc -l`.
   * This grows over time.
   */
  val ExpectedSceneListLines = 447919

  override def enabled: Boolean = super.enabled && hasCSVTestFile

  override def useCSVEndpoint: Boolean = true

  init()

  def init(): Unit = {
    if (enabled) {
      setupFilesystemConfiguration(getConf)
    }
  }

  /**
   * Iterator over remote output.
   *
   * @param source source iterator
   * @tparam T type of response
   */
  class RemoteOutputIterator[T](private val source: RemoteIterator[T])
    extends Iterator[T] {
    def hasNext: Boolean = source.hasNext

    def next: T = source.next()
  }

  /**
   * This doesn't do much, except that it is designed to be pasted straight into
   * Zeppelin and work
   */
  ctest("DirOps", "simple directory ops in spark context process") {
    val source = CSV_TESTFILE.get
    sc = new SparkContext("local", "CSVgz", newSparkConf(source))
    val landsat = "s3a://landsat-pds/scene_list.gz"
    val landsatPath = new Path(landsat)
    val fs = FileSystem.get(landsatPath.toUri, sc.hadoopConfiguration)
    val files = fs.listFiles(landsatPath.getParent, false)
    val listing = new RemoteOutputIterator(files)
    listing.foreach(print(_))
  }

  ctest("Boris", "boris bike stuff") {
    val source = CSV_TESTFILE.get
    sc = new SparkContext("local", "CSVgz", newSparkConf(source))
    val dir = "s3a://hwdev-steve-datasets-east/travel/borisbike/"
    val dirPath = new Path(dir)
    val fs = FileSystem.get(dirPath.toUri, sc.hadoopConfiguration)
    val files = fs.listFiles(dirPath, false)
    val listing = new RemoteOutputIterator(files)
    listing.foreach(print(_))

  }

  ctest("CSVToORC", "boris bike stuff") {
    val bucket = "hwdev-steve-datasets-east"
    val srcDir = s"s3a://$bucket/travel/borisbike/"
    val srcPath = new Path(srcDir)

    sc = new SparkContext("local", "CSVgz", newSparkConf(srcPath))
    val destPath = new Path(s"s3a://$bucket/travel/orc/borisbike/")
    val fs = FileSystem.get(srcPath.toUri, sc.hadoopConfiguration)
    fs.delete(destPath, true)

    val sql = SparkSession.builder().enableHiveSupport().getOrCreate()

    val range = "2013-01-01-to-2013-01-05"
    val srcDataPath = new Path(srcPath, range + ".csv.gz")
    // this is used to implicitly convert an RDD to a DataFrame.
    val csvdata = sql.read.options(Map(
      "header" -> "true",
      "timestampFormat" -> "yyyy-MM-dd HH:mm:ss.SSSZZZ",
      "inferSchema" -> "true",
      "mode" -> "FAILFAST"))
      .format("com.databricks.spark.csv")
      .load(srcDataPath.toString)
    csvdata.cache()

    /*
csvdata: org.apache.spark.sql.DataFrame = [
Rental Id: int, Duration: int,
 Bike Id: int, End Date: string, EndStation Id: int, EndStation Name: string, Start Date: string, StartStation Id: int, StartStation Name: string]
      .drop("Rental Id")
      .drop("Duration")
      .drop("Bike Id")
      .drop("End Date")
      .drop("EndStation Id")
      .drop("EndStation Name")
      .drop("Start Date")
      .drop("StartStation Id")
      .drop("StartStation Name")
 */
    import org.apache.spark.sql.functions._
    val toInt = udf[Int, String](_.toInt)
    val toDouble = udf[Double, String](_.toDouble)

    val csvDF = csvdata
      .withColumnRenamed("Rental Id", "rentalId")
      .withColumnRenamed("Duration", "duration")
      .withColumnRenamed("Bike Id", "bike")
      .withColumnRenamed("Start Date", "startDate")
      .withColumnRenamed("StartStation Id", "startStationId")
      .withColumnRenamed("StartStation Name", "startStationName")
      .withColumnRenamed("End Date", "endDate")
      .withColumnRenamed("EndStation Id", "endStationId")
      .withColumnRenamed("EndStation Name", "endStationName")

    val parquetPath = new Path(destPath, s"$range.parquet")
    csvDF.write.mode("overwrite").parquet(parquetPath.toString)

    val parquetDF = sql.read.parquet(parquetPath.toString)
    parquetDF.show()
    parquetDF.show()
    parquetDF.cache()

    parquetDF.filter("duration <= 0").show()
    val cleanedDF = parquetDF.filter("duration > 0")
    cleanedDF.show()
    cleanedDF.orderBy(asc("duration")).show()
    cleanedDF.orderBy(desc("duration")).show()

  }

}
