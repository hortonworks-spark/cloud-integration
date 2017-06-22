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

package com.hortonworks.spark.cloud.operations
import scala.language.postfixOps

import com.hortonworks.spark.cloud.ObjectStoreExample
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.scalatest.time.{Seconds, Span}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

/**
 * Test DataFrame operations using an object store as the destination and source of operations.
 * This validates the various conversion jobs all work against the object store.
 *
 * It doesn't verify timings, though some information is printed.
 */
class CloudDataFrames extends ObjectStoreExample {

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

    val dest = new Path(args(0))
    val rowCount = intArg(args, 1, 1000)
    applyObjectStoreConfigurationOptions(sparkConf, true)

    val spark = SparkSession
        .builder
        .appName("DataFrames")
        .config(sparkConf)
        .getOrCreate()

    // Ignore IDE warnings: this import is used
    import spark.implicits._

    implicit val patience = PatienceConfig(Span(30, Seconds))
    val numRows = 1000

    try {
      val sc = spark.sparkContext
      val hConf = sc.hadoopConfiguration
      // simple benchmark code from DataSetBenchmark
      val sourceData = spark.range(0, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))

      val generatedBase = new Path(dest, "generated")
      val fs = FileSystem.get(generatedBase.toUri, hConf)
      rm(fs, generatedBase)
      // formats to generate
      val formats = Seq("orc", "parquet", "json", "csv")

      // write a DF; return path and time to save
      def write(format: String): (Path, Long) = {
        val path = new Path(generatedBase, format)
        rm(fs, path)
        val d = duration2(save(sourceData, path, format))
        eventuallyListStatus(fs, path)
        d
      }

      // load a DF and verify it has the expected number of rows
      // return how long it took
      def validate(source: Path, srcFormat: String): Long = {
        validateRowCount(spark, fs, source, srcFormat, rowCount)
      }

      val results: Seq[(String, Path, Long, Long)] = formats.map { format =>
        val (written, writeTime) = write(format)
        (format, written, writeTime, validate(written, format))
      }.sortWith((l, r) => l._3 < r._3)

      logInfo("Round Trip Times")
      results.foreach { result =>
        logInfo(s"${result._1} : ${toHuman(result._3)} at ${result._2}")
      }

      val destFS = FileSystem.get(dest.toUri, hConf)

      // now there are some files in the generated directory tree. Enumerate them
      logInfo("scanning binary files")
      val base = generatedBase.toUri.toString
      val binaries = sc.binaryFiles(s"$base/orc,$base/parquet/*,$base/json,$base/csv")
      val totalSize = binaries.map(_._2.toArray().length).sum()
      logInfo(s"total size = $totalSize")

      // log any published filesystem state
      logInfo(s"FS: ${destFS}")

      extraValidation(spark, hConf, destFS, results)
    } finally {
      spark.stop()
    }
    0
  }

  /**
   * Override point for any extra validation of the output.
   *
   * @param fs filesystem used
   * @param results the list of results: (format, path, t(write), t(read))
   */
  def extraValidation(
      session: SparkSession,
      conf: Configuration,
      fs: FileSystem,
      results: Seq[(String, Path, Long, Long)]): Unit = {

  }
}

object CloudDataFrames {

  def main(args: Array[String]) {
    new CloudDataFrames().run(args)
  }

}
