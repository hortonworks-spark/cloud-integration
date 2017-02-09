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

package com.hortonworks.spark.cloud.examples

import com.hortonworks.spark.cloud._
import com.hortonworks.spark.cloud.s3.S3AConstants._
import com.hortonworks.spark.cloud.s3._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Fun with the boris bike dataset
 */
private[cloud] class BorisBikeExample extends ObjectStoreExample with S3AExampleSetup {

  /**
   * List of the command args for the current example.
   * @return a string
   */
  override protected def usageArgs(): String = {
    "<dest> [<rows>]"
  }

  /**
   * Action to execute.
   * @param sparkConf configuration to use
   * @param args argument array
   * @return an exit code
   */
  override def action(sparkConf: SparkConf,
      args: Array[String]): Int = {
    if (args.length < 1 || args.length > 2) {
      return usage()
    }
/*
    val source = S3A_CSV_PATH_DEFAULT
    val destPath = new Path(args(0))
    val srcURI = new URI(source)
    val srcPath = new Path(srcURI)

*/
    import org.apache.hadoop.fs._

    applyObjectStoreConfigurationOptions(sparkConf, false)
    hconf(sparkConf, FAST_UPLOAD, "true")
    sparkConf.set("spark.hadoop.parquet.mergeSchema", "false")
    sparkConf.set("spark.sql.parquet.filterPushdown" , "true")

    val spark = SparkSession
        .builder
        .appName("S3AIOExample")
        .config(sparkConf)
        .getOrCreate()

    val numRows = 1000

    try {
      val sc = spark.sparkContext

      val bucket = "hwdev-steve-datasets-east"
      val srcDir = s"s3a://$bucket/travel/borisbike/"
      val srcPath = new Path(srcDir)

      val destPath = new Path(s"s3a://$bucket/travel/orc/borisbike/")
      val fs = FileSystem.get(srcPath.toUri, sc.hadoopConfiguration)
      fs.delete(destPath, true)
      val srcFS = fs

      val sql = new org.apache.spark.sql.SQLContext(sc)

      val srcDataPath = new Path(srcPath, "2013-01-01-to-2013-01-05.csv.gz")
      // this is used to implicitly convert an RDD to a DataFrame.
      val csvdata = sql.read.options(Map(
        "header" -> "true",
        "ignoreLeadingWhiteSpace" -> "true",
        "ignoreTrailingWhiteSpace" -> "true",
        "timestampFormat" -> "yyyy-MM-dd HH:mm:ss.SSSZZZ",
        "inferSchema" -> "true",
        "mode" -> "FAILFAST"))
        .format("com.databricks.spark.csv")
        .load(srcDataPath.toString)

      val config = new Configuration(sc.hadoopConfiguration)
      config.set(INPUT_FADVISE, RANDOM_IO)
      val landsatPath = new Path(destPath, "landsat")
      val landsat = landsatPath.toUri.toString
      val source = landsat
      // load this FS instance into memory with random
      val destFS = FileSystem.get(destPath.toUri, config)
      destFS.delete(destPath, true)


      //  entityId,acquisitionDate,cloudCover,processingLevel,path,row,min_lat,min_lon
      // ,max_lat,max_lon,download_url
      val parquetOptions = Map(
        "mergeSchema" -> "false"
      )

      // what you need to read in the landsat data
/*
      val csvdata = spark.read.options(Map(
        "header" -> "true",
        "ignoreLeadingWhiteSpace" -> "true",
        "ignoreTrailingWhiteSpace" -> "true",
        "timestampFormat" -> "yyyy-MM-dd HH:mm:ss.SSSZZZ",
        "inferSchema" -> "true",
        "mode" -> "FAILFAST"))
          .csv("s3a://landsat-pds/scene_list.gz")
*/

      val csvOptions = Map(
        "header" -> "true",
        "timestampFormat" -> "yyyy-MM-dd HH:mm:ss.SSSZZZ",
        // double scan through file
        //        "inferSchema" -> "true",
        "mode" -> "FAILFAST"
      )

      val csv = spark.read.options(csvOptions).csv(source)
      /*
      csvdata: org.apache.spark.sql.DataFrame = [
      Rental Id: int, Duration: int,
       Bike Id: int, End Date: string, EndStation Id: int, EndStation Name: string, Start Date: string, StartStation Id: int, StartStation Name: string]
       */

      val csvDF = csvdata
        .withColumnRenamed("Rental Id", "rentalId")
        .withColumnRenamed("Duration", "duration")
        .withColumnRenamed("Bike Id", "bike")
        .withColumnRenamed("End Date", "endDate")
        .withColumnRenamed("EndStation Id", "endStationId")
        .withColumnRenamed("EndStation Name", "endStationName")
        .withColumnRenamed("Start Date", "startDate")
        .withColumnRenamed("StartStation Id", "startStationId")
        .withColumnRenamed("StartStation Name", "startStationName")
      csvDF.cache()
      csvDF.show()


      // log any published filesystem state
      print(s"\nSource: FS: $srcFS\n")
      print(s"\nDestination: FS: $destFS\n")

    } finally {
      spark.stop()
    }
    0
  }

}
