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

package org.apache.spark.sql.sources

import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.execution.DataSourceScanExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * All the tests from the orginal spark suite reworked to take a Hadoop path
 * rather than a local FS path.
 */
abstract class CloudRelationScaleTest extends AbstractCloudRelationTest {

  import testImplicits._

  ctest("save()/load() - non-partitioned table - Append",
    "", true) {
    withPath("non-part-t-append") { path =>
      val name = path.toString
      testDF.write.mode(SaveMode.Overwrite).format(dataSourceName).save(name)
      testDF.write.mode(SaveMode.Append).format(dataSourceName).save(name)

      checkAnswer(
        spark.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(name).orderBy("a"),
        testDF.union(testDF).orderBy("a").collect())
    }
  }

  ctest("save()/load() - non-partitioned table - ErrorIfExists",
    "",
    true) {
    withTempPathDir("errorIfExists") { path =>
      intercept[AnalysisException] {
        testDF.write.format(dataSourceName).mode(SaveMode.ErrorIfExists)
          .save(path.toString)
      }
    }
  }

  ctest("save()/load() - non-partitioned table - Ignore",
    "",
    true) {
    withTempPathDir("nonpartitioned") { path =>
      testDF.write.mode(SaveMode.Ignore).format(dataSourceName)
        .save(path.toString)
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      assert(fs.listStatus(path).isEmpty)
    }
  }

  ctest("save()/load() - partitioned table - simple queries",
    "",
    true) {
    withPath("simple-query") { path =>
      val p = path.toString
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.ErrorIfExists)
        .partitionBy("p1", "p2")
        .save(p)

      checkQueries(
        spark.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(p))
    }
  }

  ctest("save()/load() - partitioned table - Overwrite",
    "",
    true) {
    withPath("Overwrite") { path =>
      val name = path.toString
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(name)

      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(name)

      checkAnswer(
        spark.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(name),
        partitionedTestDF.collect())
    }
  }

  ctest("save()/load() - partitioned table - Append",
    "",
    true) {
    withPath("Append") { path =>
      val name = path.toString
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(name)

      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Append)
        .partitionBy("p1", "p2")
        .save(name)

      checkAnswer(
        spark.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(name),
        partitionedTestDF.union(partitionedTestDF).collect())
    }
  }

  ctest("save()/load() - partitioned table - Append - new partition values",
    "",
    true) {
    withPath("append-new-values") { path =>
      val name = path.toString
      partitionedTestDF1.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(name)

      partitionedTestDF2.write
        .format(dataSourceName)
        .mode(SaveMode.Append)
        .partitionBy("p1", "p2")
        .save(name)

      checkAnswer(
        spark.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(name),
        partitionedTestDF.collect())
    }
  }

  ctest("save()/load() - partitioned table - ErrorIfExists",
    "",
    true) {
    withTempPathDir("table-ErrorIfExists") { path =>
      intercept[AnalysisException] {
        partitionedTestDF.write
          .format(dataSourceName)
          .mode(SaveMode.ErrorIfExists)
          .partitionBy("p1", "p2")
          .save(path.toString)
      }
    }
  }

  ctest("save()/load() - partitioned table - Ignore",
    "",
    false) {
    withTempPathDir("ignore-partitioned-table") { path =>
      val name = path.toString
      partitionedTestDF.write
        .format(dataSourceName).mode(SaveMode.Ignore).save(name)

      assert(filesystem.listStatus(path).isEmpty)
    }
  }

  ctest("saveAsTable()/load() - non-partitioned table - Overwrite",
    "",
    false) {
    testDF.write.format(dataSourceName).mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(spark.table("t"), testDF.collect())
    }
  }

  ctest("saveAsTable()/load() - non-partitioned table - Append",
    "",
    false) {
    testDF.write.format(dataSourceName).mode(SaveMode.Overwrite)
      .saveAsTable("t")
    testDF.write.format(dataSourceName).mode(SaveMode.Append).saveAsTable("t")

    withTable("t") {
      checkAnswer(spark.table("t"), testDF.union(testDF).orderBy("a").collect())
    }
  }

  ctest("saveAsTable()/load() - non-partitioned table - ErrorIfExists",
    "",
    false) {
    withTable("t") {
      sql("CREATE TABLE t(i INT) USING parquet")
      intercept[AnalysisException] {
        testDF.write.format(dataSourceName).mode(SaveMode.ErrorIfExists)
          .saveAsTable("t")
      }
    }
  }

  ctest("saveAsTable()/load() - non-partitioned table - Ignore",
    "",
    false) {
    withTable("t") {
      sql("CREATE TABLE t(i INT) USING parquet")
      testDF.write.format(dataSourceName).mode(SaveMode.Ignore).saveAsTable("t")
      assert(spark.table("t").collect().isEmpty)
    }
  }

  ctest("saveAsTable()/load() - partitioned table - simple queries",
    "",
    false) {
    partitionedTestDF.write.format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .saveAsTable("t")

    withTable("t") {
      checkQueries(spark.table("t"))
    }
  }

  ctest("saveAsTable()/load() - partitioned table - boolean type",
    "",
    false) {
    spark.range(2)
      .select('id, ('id % 2 === 0).as("b"))
      .write.partitionBy("b").saveAsTable("t")

    withTable("t") {
      checkAnswer(
        spark.table("t").sort('id),
        Row(0, true) :: Row(1, false) :: Nil
      )
    }
  }

  ctest("saveAsTable()/load() - partitioned table - Overwrite",
    "",
    false) {
    partitionedTestDF.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    partitionedTestDF.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(spark.table("t"), partitionedTestDF.collect())
    }
  }

  ctest("saveAsTable()/load() - partitioned table - Append",
    "",
    false) {
    partitionedTestDF.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    partitionedTestDF.write
      .format(dataSourceName)
      .mode(SaveMode.Append)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(spark.table("t"),
        partitionedTestDF.union(partitionedTestDF).collect())
    }
  }

  ctest(
    "saveAsTable()/load() - partitioned table - Append - new partition values",
    "",
    false) {
    partitionedTestDF1.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    partitionedTestDF2.write
      .format(dataSourceName)
      .mode(SaveMode.Append)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(spark.table("t"), partitionedTestDF.collect())
    }
  }

  ctest(
    "saveAsTable()/load() - partitioned table - Append - mismatched partition columns",
    "",
    false) {
    partitionedTestDF1.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    // Using only a subset of all partition columns
    intercept[AnalysisException] {
      partitionedTestDF2.write
        .format(dataSourceName)
        .mode(SaveMode.Append)
        .option("dataSchema", dataSchema.json)
        .partitionBy("p1")
        .saveAsTable("t")
    }
  }

  ctest("saveAsTable()/load() - partitioned table - ErrorIfExists",
    "",
    false) {
    Seq.empty[(Int, String)].toDF().createOrReplaceTempView("t")

    withTempView("t") {
      intercept[AnalysisException] {
        partitionedTestDF.write
          .format(dataSourceName)
          .mode(SaveMode.ErrorIfExists)
          .option("dataSchema", dataSchema.json)
          .partitionBy("p1", "p2")
          .saveAsTable("t")
      }
    }
  }

  ctest("saveAsTable()/load() - partitioned table - Ignore",
    "",
    true) {
    Seq.empty[(Int, String)].toDF().createOrReplaceTempView("t")

    withTempView("t") {
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Ignore)
        .option("dataSchema", dataSchema.json)
        .partitionBy("p1", "p2")
        .saveAsTable("t")

      assert(spark.table("t").collect().isEmpty)
    }
  }

  ctest("Hadoop style globbing - unpartitioned data",
    "",
    true) {
    withPath("glob-unpartitioned") { path =>
      val dir = path.toString
      val subdir = new Path(dir, "subdir")
      val subsubdir = new Path(subdir, "subsubdir")
      val anotherSubsubdir =
        new Path(new Path(dir, "another-subdir"), "another-subsubdir")

      val dataInSubdir = Seq(1, 2, 3).toDF("value")
      val dataInSubsubdir = Seq(4, 5, 6).toDF("value")
      val dataInAnotherSubsubdir = Seq(7, 8, 9).toDF("value")

      dataInSubdir.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .save(subdir.toString)

      dataInSubsubdir.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .save(subsubdir.toString)

      dataInAnotherSubsubdir.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .save(anotherSubsubdir.toString)

      assertDirHasFiles(subdir)
      assertDirHasFiles(subsubdir)
      assertDirHasFiles(anotherSubsubdir)

      /*
        Directory structure generated

        dir
          |
          |___ subdir
          |     |
          |     |___ [ files of dataInSubdir ]
          |     |
          |     |___ subsubdir
          |               |
          |               |___ [ files of dataInSubsubdir ]
          |
          |
          |___ anotherSubdir
                |
                |___ anotherSubsubdir
                          |
                          |___ [ files of dataInAnotherSubsubdir ]
       */

      val schema = dataInSubdir.schema

      /** Check whether data is read with the given path matches the expected answer */
      def check(path: String, expectedDf: DataFrame): Unit = {
        val df = spark.read
          .format(dataSourceName)
          .schema(schema) // avoid schema inference for any format, expected to be same format
          .load(path)
        checkAnswer(df, expectedDf)
      }

      check(s"$dir/*/", dataInSubdir)
      check(s"$dir/sub*/*", dataInSubdir.union(dataInSubsubdir))
      check(s"$dir/another*/*", dataInAnotherSubsubdir)
      check(s"$dir/*/another*", dataInAnotherSubsubdir)
      check(s"$dir/*/*",
        dataInSubdir.union(dataInSubsubdir).union(dataInAnotherSubsubdir))
    }
  }

  ctest("Hadoop style globbing",
    "partitioned data with schema inference",
    true) {

    // Tests the following on partition data
    // - partitions are not discovered with globbing and without base path set.
    // - partitions are discovered with globbing and base path set, though more detailed
    //   tests for this is in ParquetPartitionDiscoverySuite

    withPath("globbing-with-schema") { path =>
      val dir = path.toString
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(dir)

      def check(
          path: String,
          expectedResult: Either[DataFrame, String],
          basePath: Option[String] = None
      ): Unit = {
        try {
          val reader = spark.read
          basePath.foreach(reader.option("basePath", _))
          val testDf = reader
            .format(dataSourceName)
            .load(path)
          assert(expectedResult.isLeft,
            s"Error was expected with $path but result found")
          checkAnswer(testDf, expectedResult.left.get)
        } catch {
          case e: java.util.NoSuchElementException if e.getMessage
            .contains("dataSchema") =>
          // Ignore error, the source format requires schema to be provided by user
          // This is needed for SimpleTextHadoopFsRelationSuite as SimpleTextSource needs schema

          case e: Throwable =>
            assert(expectedResult.isRight,
              s"Was not expecting error with $path: " + e)
            assert(
              e.getMessage.contains(expectedResult.right.get),
              s"Did not find expected error message wiht $path")
        }
      }

      object Error {
        def apply(msg: String): Either[DataFrame, String] = Right(msg)
      }

      object Result {
        def apply(df: DataFrame): Either[DataFrame, String] = Left(df)
      }

      // ---- Without base path set ----
      // Should find all the data with partitioning columns
      check(s"$dir", Result(partitionedTestDF))

      // Should fail as globbing finds dirs without files, only subdirs in them.
      check(s"$dir/*/", Error("please set \"basePath\""))
      check(s"$dir/p1=*/", Error("please set \"basePath\""))

      // Should not find partition columns as the globs resolve to p2 dirs
      // with files in them
      check(s"$dir/*/*", Result(partitionedTestDF.drop("p1", "p2")))
      check(s"$dir/p1=*/p2=foo",
        Result(partitionedTestDF.filter("p2 = 'foo'").drop("p1", "p2")))
      check(s"$dir/p1=1/p2=???",
        Result(partitionedTestDF.filter("p1 = 1").drop("p1", "p2")))

      // Should find all data without the partitioning columns as the globs resolve to the files
      check(s"$dir/*/*/*", Result(partitionedTestDF.drop("p1", "p2")))

      // ---- With base path set ----
      val resultDf = partitionedTestDF.select("a", "b", "p1", "p2")
      check(path = s"$dir/*", Result(resultDf), basePath = Some(dir))
      check(path = s"$dir/*/*", Result(resultDf), basePath = Some(dir))
      check(path = s"$dir/*/*/*", Result(resultDf), basePath = Some(dir))
    }
  }

  ctest("SPARK-9735",
    "Partition column type casting",
    false) {
    withPath("SPARK-9735") { file =>
      val df = (for {
        i <- 1 to 3
        p2 <- Seq("foo", "bar")
      } yield {
        (i, s"val_$i", 1.0d, p2, 123, 123.123f)
      }).toDF("a", "b", "p1", "p2", "p3", "f")

      val input = df.select(
        'a,
        'b,
        'p1.cast(StringType).as('ps1),
        'p2,
        'p3.cast(FloatType).as('pf1),
        'f)

      withTempView("t") {
        input
          .write
          .format(dataSourceName)
          .mode(SaveMode.Overwrite)
          .partitionBy("ps1", "p2", "pf1", "f")
          .saveAsTable("t")

        input
          .write
          .format(dataSourceName)
          .mode(SaveMode.Append)
          .partitionBy("ps1", "p2", "pf1", "f")
          .saveAsTable("t")

        val realData = input.collect()

        checkAnswer(spark.table("t"), realData ++ realData)
      }
    }
  }

  ctest("SPARK-7616",
    "adjust column name order accordingly when saving partitioned table",
    false) {
    val df = (1 to 3).map(i => (i, s"val_$i", i * 2)).toDF("a", "b", "c")

    df.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .partitionBy("c", "a")
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(spark.table("t").select('b, 'c, 'a),
        df.select('b, 'c, 'a).collect())
    }
  }

  // NOTE: This test suite is not super deterministic.  On nodes with only relatively few cores
  // (4 or even 1), it's hard to reproduce the data loss issue.  But on nodes with for example 8 or
  // more cores, the issue can be reproduced steadily.  Fortunately our Jenkins builder meets this
  // requirement.  We probably want to move this test case to spark-integration-tests or spark-perf
  // later.
  ctest("SPARK-8406",
    "Avoids name collision while writing files",
    true) {
    withPath("SPARK-8406") { dir =>
      val name = dir.toString
      spark
        .range(10000)
        .repartition(250)
        .write
        .mode(SaveMode.Overwrite)
        .format(dataSourceName)
        .save(name)

      assertResult(10000) {
        spark
          .read
          .format(dataSourceName)
          .option("dataSchema",
            StructType(StructField("id", LongType) :: Nil).json)
          .load(name)
          .count()
      }
    }
  }

  ctest("SPARK-8887",
    "Explicitly define which data types can be used as dynamic partition columns",
    true) {
    val df = Seq(
      (1, "v1", Array(1, 2, 3), Map("k1" -> "v1"), Tuple2(1, "4")),
      (2, "v2", Array(4, 5, 6), Map("k2" -> "v2"), Tuple2(2, "5")),
      (3, "v3", Array(7, 8, 9), Map("k3" -> "v3"), Tuple2(3, "6")))
      .toDF("a", "b", "c", "d", "e")
    withTempPathDir("SPARK-8887") { path =>
      intercept[AnalysisException] {
        df.write.format(dataSourceName).partitionBy("c", "d", "e")
          .save(path.toString)
      }
    }
    intercept[AnalysisException] {
      df.write.format(dataSourceName).partitionBy("c", "d", "e")
        .saveAsTable("t")
    }
  }

  ctest("Locality support for FileScanRDD",
    "",
    false) {
    val options = Map[String, String](
      "fs.file.impl" -> classOf[LocalityTestFileSystem].getName,
      "fs.file.impl.disable.cache" -> "true"
    )
    withTempPath { dir =>
      val path = dir.toURI.toString
      val df1 = spark.range(4)
      df1.coalesce(1).write.mode("overwrite").options(options)
        .format(dataSourceName).save(path)
      df1.coalesce(1).write.mode("append").options(options)
        .format(dataSourceName).save(path)

      def checkLocality(): Unit = {
        val df2 = spark.read
          .format(dataSourceName)
          .option("dataSchema", df1.schema.json)
          .options(options)
          .load(path)

        val Some(fileScanRDD) = df2.queryExecution.executedPlan.collectFirst {
          case scan: DataSourceScanExec if scan.inputRDDs().head
            .isInstanceOf[FileScanRDD] =>
            scan.inputRDDs().head.asInstanceOf[FileScanRDD]
        }

        val partitions = fileScanRDD.partitions
        val preferredLocations = partitions
          .flatMap(fileScanRDD.preferredLocations)

        assert(preferredLocations.distinct.length == 2)
      }

      checkLocality()

      withSQLConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD.key -> "0") {
        checkLocality()
      }
    }
  }

  ctest("SPARK-16975",
    "Partitioned table with the column having '_' should be read correctly",
    true) {
    withTempPathDir("SPARK-16975") { dir =>
      val childDir = new Path(dir, dataSourceName)
      val dataDf = spark.range(10).toDF()
      val df = dataDf.withColumn("_col", $"id")
      df.write.format(dataSourceName).partitionBy("_col")
        .save(childDir.toString)
      val reader = spark.read.format(dataSourceName)

      // This is needed for SimpleTextHadoopFsRelationSuite as SimpleTextSource needs schema.
      if (dataSourceName == classOf[SimpleTextSource].getCanonicalName) {
        reader.option("dataSchema", dataDf.schema.json)
      }
      val readBack = reader.load(childDir.toString)
      checkAnswer(df, readBack)
    }
  }

  /* from org.apache.spark.sql.execution.datasources.HadoopFsRelationSuite

  ctest("sizeInBytes should be the total size of all files") {
    withTempDir { dir =>
      dir.delete()
      spark.range(1000).write.parquet(dir.toString)
      // ignore hidden files
      val allFiles = dir.listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          !name.startsWith(".") && !name.startsWith("_")
        }
      })
      val totalSize = allFiles.map(_.length()).sum
      val df = spark.read.parquet(dir.toString)
      assert(df.queryExecution.logical.stats(sqlConf).sizeInBytes ===
        BigInt(totalSize))
    }
  }
*/
}
