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

import com.hortonworks.spark.cloud.s3.S3ATestSetup
import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.execution.DataSourceScanExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

abstract class CloudRelationBasicSuite extends AbstractCloudRelationTest with S3ATestSetup {

import testImplicits._

  ctest("save()/load() - non-partitioned table - Overwrite",
    "",
    true) {
    withPath("non-part-t-overwrite") { path =>
      val name = path.toString
      testDF.write.mode(SaveMode.Overwrite).format(dataSourceName).save(name)
      testDF.write.mode(SaveMode.Overwrite).format(dataSourceName).save(name)

      checkAnswer(
        spark.read.format(dataSourceName)
          .option("path", name)
          .option("dataSchema", dataSchema.json)
          .load(),
        testDF.collect())
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

  ctest("load() - with directory of unpartitioned data in nested subdirs",
    "",
    true) {
    withPath("nested") { dir =>
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

      // Inferring schema should throw error as it should not find any file to infer
      val e = intercept[Exception] {
        spark.read.format(dataSourceName).load(dir.toString)
      }

      e match {
        case _: AnalysisException =>
          assert(e.getMessage.contains("infer"))

        case _: java.util.NoSuchElementException if e.getMessage
          .contains("dataSchema") =>
        // Ignore error, the source format requires schema to be provided by user
        // This is needed for SimpleTextHadoopFsRelationSuite as SimpleTextSource needs schema

        case _ =>
          fail("Unexpected error trying to infer schema from empty dir", e)
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

      // Verify that if there is data in dir, then reading by path 'dir/' reads only dataInDir
      dataInDir.write
        .format(dataSourceName)
        .mode(SaveMode.Append) // append to prevent subdir from being deleted
        .save(dir.toString)
      assertDirHasFiles(dir)

      require(ls(subdir, true).exists(!_.isDirectory))
      assertDirHasFiles(subdir)
      testWithPath(dir, dataInDir.collect())
    }
  }

}
