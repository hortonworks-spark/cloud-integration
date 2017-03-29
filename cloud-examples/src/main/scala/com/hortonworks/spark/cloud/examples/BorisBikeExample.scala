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
import org.apache.spark.sql.types.DataTypes._

/**
 * Fun with the boris bike dataset
 */
class BorisBikeExample extends ObjectStoreExample with S3AExampleSetup {

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
/*
    if (args.length < 2 || args.length > 2) {
      return usage()
    }

*/

    def argPath(index: Int): Option[String] = {
      if (args.length > index)
        Some(args(index))
      else
        None
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

      val srcBucket = "hwdev-steve-datasets-east"

      val srcDir = argPath(0).getOrElse(
        s"s3a://$srcBucket/travel/borisbike/")
      val srcPath = new Path(srcDir)


      val destBucket = "hwdev-steve-london"
      val destDir = argPath(1)
        .getOrElse(s"s3a://$srcBucket/travel/orc/borisbike/")
      val destPath = new Path(destDir)
      val fs = srcPath.getFileSystem(sc.hadoopConfiguration)
      fs.delete(destPath, true)
      val srcFS = fs

      val sql = new org.apache.spark.sql.SQLContext(sc)



      val config = new Configuration(sc.hadoopConfiguration)
      config.set(INPUT_FADVISE, RANDOM_IO)
      // load this FS instance into memory with random
      val destFS = destPath.getFileSystem(config)
      destFS.delete(destPath, true)


      val parquetOptions = Map(
        "mergeSchema" -> "false"
      )

      val csvOptions = Map(
        "header" -> "true",
        "ignoreLeadingWhiteSpace" -> "true",
        "ignoreTrailingWhiteSpace" -> "true",
        "timestampFormat" -> "yyyy-MM-dd HH:mm:ss.SSSZZZ",
        "inferSchema" -> "true",
        "mode" -> "FAILFAST")
      // this is used to implicitly convert an RDD to a DataFrame.
      val csv = sql.read.options(csvOptions)
        .csv(srcPath.toString)
      /*
      csvdata: org.apache.spark.sql.DataFrame = [
      Rental Id: int, Duration: int,
       Bike Id: int, End Date: string, EndStation Id: int, EndStation Name: string, Start Date: string, StartStation Id: int, StartStation Name: string]
       */

      val csvDF = csv
        .withColumnRenamed("Rental Id", "rental")
        .withColumn("rental", csv.col("Rental Id").cast(IntegerType))
        .withColumn("duration", csv.col("Duration").cast(IntegerType))
        .withColumnRenamed("Bike Id", "bike")
        .withColumnRenamed("End Date", "endDate")
        .withColumnRenamed("EndStation Id", "endStation")
        .withColumnRenamed("EndStation Name", "endStationName")
        .withColumnRenamed("Start Date", "startDate")
        .withColumnRenamed("StartStation Id", "startStationId")
        .withColumnRenamed("StartStation Name", "startStationName")
      csvDF.repartition(8).cache()
      val sourceRowCount = csvDF.count()
      println(s"defaultParallelism ${sc.defaultParallelism}")
      println(s"source: $srcPath contains ${sourceRowCount} rows")
      csvDF.show()

      save(csvDF, destPath, "orc")

      val orcDF = sql.read.orc(destPath.toString);
      orcDF.show()
      val orcRowCount = orcDF.count()
      val generatedFiles = destFS.listStatus(destPath)
        .filter(!_.getPath.getName.startsWith("_"))
      generatedFiles.foreach( s =>
      println(s"${s.getPath.getName} size = ${s.getLen/1024} KB"))

      println(s"ORC: $destPath contains ${orcRowCount} rows")

      // log any published filesystem state
      print(s"\nSource: FS: $srcFS\n")
      print(s"\nDestination: FS: $destFS\n")

    } finally {
      spark.stop()
    }
    0
  }

}

object BorisBikeExample {

  def main(args: Array[String]) {
    new BorisBikeExample().run(args)
  }

}
