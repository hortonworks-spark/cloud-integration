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

import scala.util.Random

import com.hortonworks.spark.cloud.s3.S3ACommitterConstants
import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Basic suite of cloud relations; speed over coverage.
 */
abstract class CloudRelationBasicSuite extends AbstractCloudRelationTest {

import testImplicits._

  for (dataType <- parquetDataTypes) {
    for (parquetDictionaryEncodingEnabled <- Seq(true, false)) {
      ctest(
        s"test all data types - $dataType with parquet.enable.dictionary = " +
          s"$parquetDictionaryEncodingEnabled", "", false) {

        val extraOptions = Map[String, String](
          "parquet.enable.dictionary" ->
            parquetDictionaryEncodingEnabled.toString
        )

        withTempPathDir(s"datatype-$dataType") { path =>
          val pathname = path.toUri.toString

          val dataGenerator = RandomDataGenerator.forType(
            dataType = dataType,
            nullable = true,
            new Random(System.nanoTime())
          ).getOrElse {
            fail(s"Failed to create data generator for schema $dataType")
          }

          // Create a DF for the schema with random data. The index field is used to sort the
          // DataFrame.  This is a workaround for SPARK-10591.
          val schema = new StructType()
            .add("index", IntegerType, nullable = false)
            .add("col", dataType, nullable = true)
          val rdd =
            spark.sparkContext
              .parallelize((1 to 10).map(i => Row(i, dataGenerator())))
          val df = spark.createDataFrame(rdd, schema).orderBy("index")
            .coalesce(1)

          df.write
            .mode("overwrite")
            .format(dataSourceName)
            .option("dataSchema", df.schema.json)
            .options(extraOptions)
            .save(pathname)

          val loadedDF = spark
            .read
            .format(dataSourceName)
            .option("dataSchema", df.schema.json)
            .schema(df.schema)
            .options(extraOptions)
            .load(pathname)
            .orderBy("index")

          checkAnswer(loadedDF, df)
        }
      }
    }
  }


  ctest("save()/findClass() - non-partitioned table - Overwrite",
    "write to a non-parted table overwrite=true",
    true) {
    assertSparkRunning()
    withPath("non-part-table-overwrite") { path =>
      val dest = path.toString
      logInfo(s"Writing to output path $dest in format $dataSourceName")
      testDF.write.mode(SaveMode.Overwrite).format(dataSourceName).save(dest)
      logInfo(s"Second attempt")
      testDF.write.mode(SaveMode.Overwrite).format(dataSourceName).save(dest)

      checkAnswer(
        spark.read.format(dataSourceName)
          .option("path", dest)
          .option("dataSchema", dataSchema.json)
          .load(),
        testDF.collect())
    }
  }

  ctest("save()/findClass() - non-partitioned table - ErrorIfExists",
    "Expect an error trying to write to a directory which exists",
    true) {
    assertSparkRunning()
    withTempPathDir("non-parted-error-if-exists", None) { path =>
      intercept[AnalysisException] {
        testDF.write
          .format(dataSourceName)
          .mode(SaveMode.ErrorIfExists)
          .save(path.toString)
      }
    }
  }

  ctest("findClass() - with directory of unpartitioned data in nested subdirs",
    "",
    true) {
    assertSparkRunning()
    withPath("unparted-nested", None) { dir =>
      val subdir = new Path(dir, "subdir")

      val dataInDir = Seq(1, 2, 3).toDF("value")
      val dataInSubdir = Seq(4, 5, 6).toDF("value")

      /*

        Directory structure to be generated

        dir
          |
          |___ [ files of dataInDir ]
          |
          |___ subsubdir
                    |
                    |___ [ files of dataInSubdir ]
      */

      // Generated dataInSubdir, not data in dir
      dataInSubdir.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .save(subdir.toString)
      // TODO: check for non zero success file in subdir
      assertSuccessFileExists(subdir)
      // Inferring schema should throw error as it should not find any file to infer
      val e = intercept[Exception] {
        spark.read.format(dataSourceName).load(dir.toString)
      }

      e match {
        case m: AnalysisException =>
           if (!m.getMessage.contains("infer")) {
             fail("AnalysisException doesn't contain message 'infer'", m)
           }

        case _: java.util.NoSuchElementException if e.getMessage.contains("dataSchema") =>
        // Ignore error, the source format requires schema to be provided by user
        // This is needed for SimpleTextHadoopFsRelationSuite as SimpleTextSource needs schema

        case _ =>
          fail(s"Unexpected error trying to infer schema from empty dir $e", e)
      }

      /** Test whether data is read with the given path matches the expected answer */
      def testWithPath(path: Path, expectedAnswer: Seq[Row]): Unit = {
        val df = spark.read
          .format(dataSourceName)
          .schema(dataInDir.schema) // avoid schema inference for any format
          .load(path.toString)
        checkAnswer(df, expectedAnswer)
      }

      // Verify that reading by path 'dir/' gives empty results as there are no files in 'file'
      // and it should not pick up files in 'dir/subdir'
      assertPathExists(subdir)

      assertDirHasFiles(subdir)

      testWithPath(dir, Seq.empty)

      logInfo("Appending data to base directory")
      // Verify that if there is data in dir, then reading by path 'dir/' reads only dataInDir

      dataInDir.write
        .format(dataSourceName)
        .mode(SaveMode.Append) // append to prevent subdir from being deleted
        .option(S3ACommitterConstants.CONFLICT_MODE,
        S3ACommitterConstants.CONFLICT_MODE_APPEND)   // and for s3 committers
        .save(dir.toString)
      assertDirHasFiles(dir)
      assertSuccessFileExists(dir)

      require(ls(subdir, true).exists(!_.isDirectory))
      assertDirHasFiles(subdir)
      testWithPath(dir, dataInDir.collect())
    }
  }

}
