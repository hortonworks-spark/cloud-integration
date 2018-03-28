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
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Test of a single operation; isolated for debugging.
 */
abstract class CloudPartitionTest extends AbstractCloudRelationTest {

import testImplicits._

  ctest(
    "save-load-partitioned-part-columns-in-data",
    "Save sets of files in explicitly set up partition tree; read") {
    withTempPathDir("part-columns", None) { path =>
      for (p1 <- 1 to 2; p2 <- Seq("foo", "bar")) {
        val partitionDir = new Path(path, s"p1=$p1/p2=$p2")
        val df = sparkContext
          .parallelize(for (i <- 1 to 3) yield (i, s"val_$i", p1))
          .toDF("a", "b", "p1")

         df.write
          .format(dataSourceName)
          .mode(SaveMode.ErrorIfExists)
          .save(partitionDir.toString)
        // each of these directories as its own success file; there is
        // none at the root
        resolveSuccessFile(partitionDir, true)
      }

      val dataSchemaWithPartition =
        StructType(
          dataSchema.fields :+ StructField("p1", IntegerType, nullable = true))

      checkQueries(
        spark.read.options(Map(
          "path" -> path.toString,
          "dataSchema" -> dataSchemaWithPartition.json)).format(dataSourceName)
          .load())
    }
  }
}
