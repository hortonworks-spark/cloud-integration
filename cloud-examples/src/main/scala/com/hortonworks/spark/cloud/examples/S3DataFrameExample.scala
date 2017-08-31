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

import java.net.URI

import com.hortonworks.spark.cloud._
import com.hortonworks.spark.cloud.s3._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes._

/**
 * Pull in Landsat CSV and convert to Parquet & ORC formats; do a couple of operations
 * round things out.
 *
 * See <a href="http://landsat.usgs.gov/tools_faq.php">Landsat Web Site</a>
 * for details about terminology and raw data.
 *
 * header:
 * {{{
 *   entityId: string : LC80101172015002LGN00
 *   acquisitionDate: timestamp: 2015-01-02 15:49:05.571384
 *   cloudCover: double: 80.81
 *   processingLevel: string: L1GT
 *   path: integer: 10
 *   row: integer 117
 *   min_lat: double: -79.09923
 *   min_lon: double: -139.6608
 *   max_lat: double: -77.7544
 *   max_lon: double: -125.09297
 *   download_url: string
 *     https://s3-us-west-2.amazonaws.com/landsat-pds/L8/010/117/LC80101172015002LGN00/index.html
 *
 * }}}
 */
class S3DataFrameExample extends ObjectStoreExample with S3AExampleSetup {

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
    val source = S3A_CSV_PATH_DEFAULT
    val destPath = new Path(args(0))
    val srcURI = new URI(source)
    val srcPath = new Path(srcURI)

    applyObjectStoreConfigurationOptions(sparkConf, false)
    hconf(sparkConf, FAST_UPLOAD, "true")
    sparkConf.set("spark.hadoop.parquet.mergeSchema", "false")
    sparkConf.set("spark.sql.parquet.filterPushdown" , "true")

    val spark = SparkSession
        .builder
        .appName("S3AIOExample")
        .config(sparkConf)
        .config("spark.master", "local")
        .getOrCreate()

    val numRows = 1000

    try {
      val sc = spark.sparkContext
      sc.hadoopConfiguration.set("parquet.mergeSchema", "false")

      val config = new Configuration(sc.hadoopConfiguration)

      // for the source CSV file random IO means too many
      // small files,
      config.set(INPUT_FADVISE, NORMAL_IO)
      val srcFS = srcPath.getFileSystem(config)
      val sourceFileStatus = srcFS.getFileStatus(srcPath)

      // turn on random access. for the destination FS
      config.set(INPUT_FADVISE, RANDOM_IO)
      val landsatPath = new Path(destPath, "landsat")
      val landsatOrcPath = new Path(landsatPath, "orc")
      val landsatParqetPath = new Path(landsatPath, "parquet")
      val landsatParqet = landsatParqetPath.toString
      val landsat = landsatPath.toUri.toString
      // load this FS instance into memory with random
      val destFS = destPath.getFileSystem(config)
      rm(destFS, landsatPath)

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
        "ignoreLeadingWhiteSpace" -> "true",
        "ignoreTrailingWhiteSpace" -> "true",
        "timestampFormat" -> "yyyy-MM-dd HH:mm:ss.SSSZZZ",
        // double scan through file
        //        "inferSchema" -> "true",
        "mode" -> "FAILFAST"
      )
      val csv = spark.read.options(csvOptions).csv(source)
      val csvData = csv
          .withColumnRenamed("entityId", "id")
          .withColumn("acquisitionDate", csv.col("acquisitionDate").cast(TimestampType))
          .withColumn("cloudCover", csv.col("cloudCover").cast(DoubleType))
          .withColumn("path", csv.col("path").cast(IntegerType))
          .withColumn("row", csv.col("row").cast(IntegerType))
          .withColumn("min_lat", csv.col("min_lat").cast(DoubleType))
          .withColumn("min_lon", csv.col("min_lon").cast(DoubleType))
          .withColumn("max_lat", csv.col("max_lat").cast(DoubleType))
          .withColumn("max_lon", csv.col("max_lon").cast(DoubleType))

      csvData.cache()

//      val landsat = "s3a://hwdev-stevel-demo/landsat"
      csvData.write.parquet(landsatParqetPath.toString)
      csvData.write.orc(landsatOrcPath.toString)

      /*
      val landsatOrc = "s3a://hwdev-stevel-demo/landsatOrc"
      csvData.write.mode("overwrite").orc(landsatOrc)
*/

      val files = listFiles(destFS, destPath, true)
      files.foreach(file =>
        print(s"${file.getPath}: [${file.getLen}]")
      )
      val totalOutputSize = files.map(_.getLen).sum
      // the generated file will be bigger due to snappy vs gzip, and more critically
      // all fields are being described as string...you'd need to parse the timestamp
      // and convert the lat/long values to doubles
      print(s"Input size ${sourceFileStatus.getLen}; output size = $totalOutputSize")

      sc.hadoopConfiguration.set("parquet.mergeSchema", "false")
      sparkConf.set("spark.sql.parquet.mergeSchema", "false")

      val df = spark.read
          .option("mergeSchema", "false")
          .option("filterPushdown", "true")
           // filter("cloudCover < 30")
          .parquet(landsatParqet)
          .where("cloudcover >= 100")


      // SQL queries on the parquet data
      df.describe().show(10, true)

      val sqlDF = spark.sql(s"SELECT id, acquisitionDate,cloudCover" +
          s" FROM parquet.`${landsatParqetPath}`")
      val negativeClouds = sqlDF.filter("cloudCover < 30")
//      println(s"${negativeClouds.count()} entries with negative cloud cover")
      negativeClouds.show()

      // log any published filesystem state
      print(s"\nSource: FS: $srcFS\n")
      print(s"\nDestination: FS: $destFS\n")

    } finally {
      spark.stop()
    }
    0
  }

}

object S3DataFrameExample {

  def main(args: Array[String]) {
    new S3DataFrameExample().run(args)
  }

}
