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

import java.io.FileNotFoundException

import com.hortonworks.spark.cloud._
import com.hortonworks.spark.cloud.s3.S3AConstants._
import com.hortonworks.spark.cloud.s3._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.functions._
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

  val _rental = "rental"

  val _duration = "time"

  val _bike = "bike"

  val _ended = "ended"

  val _endstation = "endstation"

  val _endstation_name = "endstation_name"

  val _started = "started"

  val _startstation = "startstation"

  val _startstation_name = "startstation_name"

  // Rental Id,Duration,Bike Id,End Date,EndStation Id,EndStation Name,Start Date,StartStation Id,StartStation Name
  def schema = Map(
    "Rental Id" -> (IntegerType, _rental),
    "Duration" -> (IntegerType, _duration),
    "Bike Id" -> (IntegerType, _bike),
    "End Date" -> (StringType, _ended),
    "EndStation Id" -> (IntegerType, _endstation),
    "EndStation Name" -> (StringType, _endstation_name),
    "Start Date" -> (StringType, _started),
    "StartStation Id" -> (IntegerType, _startstation),
    "StartStation Name" -> (StringType, _startstation_name)
  )

  /**
   * Action to execute.
   * @param sparkConf configuration to use
   * @param args argument array
   * @return an exit code
   */
  override def action(sparkConf: SparkConf,
      args: Array[String]): Int = {

    def argPath(index: Int): Option[String] = {
      if (args.length > index) {
        Some(args(index))
      } else {
        None
      }
    }

    args.foreach(a => println(a))

/*
    val source = S3A_CSV_PATH_DEFAULT
    val destPath = new Path(args(0))
    val srcURI = new URI(source)
    val srcPath = new Path(srcURI)

*/

    sparkConf.set("spark.default.parallelism", "4")
    applyObjectStoreConfigurationOptions(sparkConf, false)
    hconf(sparkConf, FAST_UPLOAD, "true")
    sparkConf.set("spark.hadoop.parquet.mergeSchema", "false")
    sparkConf.set("spark.sql.parquet.filterPushdown" , "true")

    hconf(sparkConf, OUTPUTCOMMITTER_FACTORY_CLASS,
      "org.apache.hadoop.fs.s3a.commit.staging.PartitonedStagingCommitterFactory")

    val spark = SparkSession
        .builder
        .appName("BorisBike1")
        .config(sparkConf)
        .getOrCreate()

    val srcBucket = "hwdev-steve-datasets-east"

    val srcDir = argPath(0).getOrElse(
      s"s3a://$srcBucket/travel/borisbike/")
    val srcPath = new Path(srcDir)

    val destBucket = "hwdev-steve-london"
    val destDir = argPath(1)
      .getOrElse(s"s3a://$srcBucket/travel/orc/borisbike/")
    val destPath = new Path(destDir)


    try {
      val sc = spark.sparkContext
      val sql = new org.apache.spark.sql.SQLContext(sc)

      val config = new Configuration(sc.hadoopConfiguration)
      config.set(INPUT_FADVISE, RANDOM_IO)
      // load this FS instance into memory with random
      val destFS = destPath.getFileSystem(config)
      try {
        destFS.listStatus(destPath)
      } catch {
        case fnfe: FileNotFoundException =>
          destFS.delete(destPath, true)
      }

      importFromCSV(spark, sc, sql, srcPath, destPath)
      simpleOperations(spark, sc, sql, destPath, destPath)

      // log any published filesystem state
      println(s"Source: FS: ${srcPath.getFileSystem(sc.hadoopConfiguration)}")
      println(s"Destination: FS: ${ destPath.getFileSystem(sc.hadoopConfiguration)}")
    } finally {
      spark.stop()
    }
    0
  }


  /**
   * The import routine, if you want to do it again
   */
  def importFromCSV(
      spark: SparkSession,
      sc: SparkContext,
      sql: SQLContext,
      srcPath: Path,
      destPath: Path): Unit = {
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
      "timestampFormat" -> "dd-MM-yyyy HH:mm",
      "inferSchema" -> "false",
      "mode" -> "DROPMALFORMED")
    // this is used to implicitly convert an RDD to a DataFrame.
    val rawCsv = sql.read.options(csvOptions)
      .csv(srcPath.toString)

    rawCsv.show()
    /*
  csvdata: org.apache.spark.sql.DataFrame = [
  Rental Id: int, Duration: int,
   Bike Id: int, End Date: string, EndStation Id: int, EndStation Name: string, Start Date: string, StartStation Id: int, StartStation Name: string]
   */

    def col(
        df: DataFrame,
        newname: String,
        oldname: String,
        t: DataType): DataFrame = {
      df.withColumn(newname, df.col(oldname).cast(t)).drop(oldname)
    }



    var df = rawCsv;
    schema.foreach {
      (entry) =>
        df = col(df, entry._2._2, entry._1, entry._2._1)
    }

    val csvDF = df


    val sourceRowCount = csvDF.count()
    println(s"defaultParallelism ${sc.defaultParallelism}")
    println(s"source: $srcPath contains ${sourceRowCount} rows")
    csvDF.show()

    logInfo(s"Saving as ORC To $destPath")
    save(csvDF, destPath, "orc")

    logInfo(s"Reading ORC from $destPath")

    val orcDF = sql.read.orc(destPath.toString);
    orcDF.show()
    val orcRowCount = orcDF.count()
    val generatedFiles = destFS.listStatus(destPath)
      .filter(!_.getPath.getName.startsWith("_"))
    generatedFiles.foreach(s =>
      println(s"${s.getPath.getName} size = ${s.getLen / 1024} KB"))

    println(s"ORC: $destPath contains ${orcRowCount} rows")

  }

  def simpleOperations(
      spark: SparkSession,
      sc: SparkContext,
      sql: SQLContext,
      orcPath: Path,
      destPath: Path): Unit = {
    import sql.implicits._
    val orcDF = sql.read.orc(orcPath.toString);
    orcDF.show()
    val coreDF = orcDF
      .select(_bike, _duration, _startstation_name, _endstation_name)
    coreDF.createGlobalTempView("boris")

    coreDF
      .groupBy(_startstation_name).count().sort($"count".desc).show()

    spark.sql("SELECT * FROM global_temp.boris").show()

  }
}

object BorisBikeExample {

  def main(args: Array[String]) {
    new BorisBikeExample().run(args)
  }

}