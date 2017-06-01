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

package org.apache.spark.sql.hive.orc.cloud


import com.hortonworks.spark.cloud.s3.S3ATestSetup
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.HadoopCloudRelationTest
import org.apache.spark.sql.types._

class S3AOrcRelationSuite extends HadoopCloudRelationTest
  with S3ATestSetup {
import testImplicits._

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
    }
  }

  /**
   * Switch to random IO if the s3a implementation supports it.
   *
   * @return the IO type
   */
  override protected def inputPolicy: String = RANDOM_IO

  override val dataSourceName: String = classOf[OrcFileFormat].getCanonicalName

  // ORC does not play well with NullType and UDT.
  override protected def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: NullType => false
    case _: CalendarIntervalType => false
    case _: UserDefinedType[_] => false
    case _ => true
  }

  ctest("save()/load() - partitioned table - simple queries - partition columns in data",
    "",
    true) {
    withTempPathDir("part-colums") { path =>
      for (p1 <- 1 to 2; p2 <- Seq("foo", "bar")) {
        val partitionDir = new Path(path, s"p1=$p1/p2=$p2")
        sparkContext
          .parallelize(for (i <- 1 to 3) yield (i, s"val_$i", p1))
          .toDF("a", "b", "p1")
          .write
          .orc(partitionDir.toString)
      }

      val dataSchemaWithPartition =
        StructType(dataSchema.fields :+ StructField("p1", IntegerType, nullable = true))

      checkQueries(
        spark.read.options(Map(
          "path" -> path.toString,
          "dataSchema" -> dataSchemaWithPartition.json)).format(dataSourceName).load())
    }
  }

  ctest("SPARK-12218",
    "'Not' is included in ORC filter pushdown", false) {

    withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPathDir("SPARK-12218") { dir =>
        val path = s"${dir.toString}/table1"
        (1 to 5).map(i => (i, (i % 2).toString)).toDF("a", "b").write.orc(path)

        checkAnswer(
          spark.read.orc(path).where("not (a = 2) or not(b in ('1'))"),
          (1 to 5).map(i => Row(i, (i % 2).toString)))

        checkAnswer(
          spark.read.orc(path).where("not (a = 2 and b in ('1'))"),
          (1 to 5).map(i => Row(i, (i % 2).toString)))
      }
    }
  }


}
