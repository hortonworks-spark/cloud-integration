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

import java.io.File

import scala.util.Random

import com.hortonworks.spark.cloud.CloudSuiteTrait
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{RandomDataGenerator, _}
import org.apache.spark.util.Utils


/**
 * Minimal base class for cloud relation tests.
 *
 * See: org.apache.spark.sql.sources.HadoopFsRelationTest
 */
abstract class AbstractCloudRelationTest extends QueryTest with SQLTestUtils
  with TestHiveSingleton with CloudSuiteTrait with BeforeAndAfterAll {

  import spark.implicits._

  val dataSourceName: String

  protected override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      if (spark != null) {
        logInfo("Closing spark context")
        spark.stop()
        spark == null
      }
    }
  }

  def assertSparkRunning(): Unit = {
    assert(spark != null, "No spark context")
  }

  /**
   * Datatype mapping is for ORC; other formats may override this.
 *
   * @param dataType
   * @return
   */
  protected def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: NullType => false
    case _: CalendarIntervalType => false
    case _: UserDefinedType[_] => false
    case _ => true
  }

  val dataSchema =
    StructType(
      Seq(
        StructField("a", IntegerType, nullable = false),
        StructField("b", StringType, nullable = false)))

  lazy val testDF = spark.range(1, 3).map(i => (i, s"val_$i")).toDF("a", "b")

  lazy val partitionedTestDF1 = (for {
    i <- 1 to 3
    p2 <- Seq("foo", "bar")
  } yield {
    (i, s"val_$i", 1, p2)
  }).toDF("a", "b", "p1", "p2")

  lazy val partitionedTestDF2 = (for {
    i <- 1 to 3
    p2 <- Seq("foo", "bar")
  } yield {
    (i, s"val_$i", 2, p2)
  }).toDF("a", "b", "p1", "p2")

  lazy val partitionedTestDF = partitionedTestDF1.union(partitionedTestDF2)

  /**
   * Generates a temporary path without creating the actual file/directory, pass
   * it to the function, then cleanup with a deleteQuietly().
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected def withPath(name: String)(f: Path => Unit): Unit = {
    val dir = path(name)
    try {
      f(dir)
    } finally {
      // wait for all tasks to finish before deleting files
      waitForTasksToFinish()
      deleteQuietly(dir)
    }
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected def withTempPathDir(name: String)(f: Path => Unit): Unit = {
    val dir = path(name)
    filesystem.mkdirs(dir)
    try {
      f(dir)
    } finally {
      // wait for all tasks to finish before deleting files
      waitForTasksToFinish()
      deleteQuietly(dir)
    }
  }


  /**
   * Generates a temporary path without creating the actual file/directory, then pass it to `f`. If
   * a file/directory is created there by `f`, it will be delete after `f` returns.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected override def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try {
      f(path)
    } finally {
      Utils.deleteRecursively(path)
    }
  }

  def checkQueries(df: DataFrame): Unit = {
    // Selects everything
    checkAnswer(
      df,
      for (i <- 1 to 3; p1 <- 1 to 2; p2 <- Seq("foo", "bar")) yield {
        Row(i, s"val_$i", p1, p2)
      })

    // Simple filtering and partition pruning
    checkAnswer(
      df.filter('a > 1 && 'p1 === 2),
      for (i <- 2 to 3; p2 <- Seq("foo", "bar")) yield {
        Row(i, s"val_$i", 2, p2)
      })

    // Simple projection and filtering
    checkAnswer(
      df.filter('a > 1).select('b, 'a + 1),
      for (i <- 2 to 3; _ <- 1 to 2; _ <- Seq("foo", "bar")) yield {
        Row(s"val_$i", i + 1)
      })

    // Simple projection and partition pruning
    checkAnswer(
      df.filter('a > 1 && 'p1 < 2).select('b, 'p1),
      for (i <- 2 to 3; _ <- Seq("foo", "bar")) yield {
        Row(s"val_$i", 1)
      })


    // Project many copies of columns with different types (reproduction for SPARK-7858)
    checkAnswer(
      df.filter('a > 1 && 'p1 < 2).select('b, 'b, 'b, 'b, 'p1, 'p1, 'p1, 'p1),
      for (i <- 2 to 3; _ <- Seq("foo", "bar"))
        yield {
          Row(s"val_$i", s"val_$i", s"val_$i", s"val_$i", 1, 1, 1, 1)
        })

    // Self-join
    df.createOrReplaceTempView("t")
    withTempView("t") {
      checkAnswer(
        sql(
          """SELECT l.a, r.b, l.p1, r.p2
            |FROM t l JOIN t r
            |ON l.a = r.a AND l.p1 = r.p1 AND l.p2 = r.p2
          """.stripMargin),
        for (i <- 1 to 3; p1 <- 1 to 2; p2 <- Seq("foo", "bar")) yield {
          Row(i, s"val_$i", p1, p2)
        })
    }
  }

  private val supportedDataTypes = Seq(
    StringType, BinaryType,
    NullType, BooleanType,
    ByteType, ShortType, IntegerType, LongType,
    FloatType, DoubleType, DecimalType(25, 5), DecimalType(6, 5),
    DateType, TimestampType,
    ArrayType(IntegerType),
    MapType(StringType, LongType),
    new StructType()
      .add("f1", FloatType, nullable = true)
      .add("f2", ArrayType(BooleanType, containsNull = true), nullable = true),
    new UDT.MyDenseVectorUDT()
  ).filter(supportsDataType)

  private val orcSupportedDataTypes = Seq(
    StringType, BinaryType,
    NullType, BooleanType,
    ByteType, ShortType, IntegerType, LongType,
    FloatType, DoubleType, DecimalType(25, 5), DecimalType(6, 5),
    DateType, TimestampType,
    ArrayType(IntegerType),
    MapType(StringType, LongType),
    new StructType()
      .add("f1", FloatType, nullable = true)
      .add("f2", ArrayType(BooleanType, containsNull = true), nullable = true),
    new UDT.MyDenseVectorUDT()
  )

  for (dataType <- supportedDataTypes) {
    for (parquetDictionaryEncodingEnabled <- Seq(true, false)) {
      ctest(s"test all data types - $dataType with parquet.enable.dictionary = " +
        s"$parquetDictionaryEncodingEnabled", "", false) {

        val extraOptions = Map[String, String](
          "parquet.enable.dictionary" -> parquetDictionaryEncodingEnabled.toString
        )

        withTempPath { file =>
          val path = file.toString

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
            spark.sparkContext.parallelize((1 to 10).map(i => Row(i, dataGenerator())))
          val df = spark.createDataFrame(rdd, schema).orderBy("index").coalesce(1)

          df.write
            .mode("overwrite")
            .format(dataSourceName)
            .option("dataSchema", df.schema.json)
            .options(extraOptions)
            .save(path)

          val loadedDF = spark
            .read
            .format(dataSourceName)
            .option("dataSchema", df.schema.json)
            .schema(df.schema)
            .options(extraOptions)
            .load(path)
            .orderBy("index")

          checkAnswer(loadedDF, df)
        }
      }
    }
  }


}




