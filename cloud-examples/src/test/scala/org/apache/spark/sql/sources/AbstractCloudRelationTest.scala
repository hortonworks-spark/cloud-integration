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

import scala.concurrent.duration._

import com.hortonworks.spark.cloud.{CloudSuiteTrait, ObjectStoreConfigurations}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


/**
 * Minimal base class for cloud relation tests.
 *
 * See: org.apache.spark.sql.sources.HadoopFsRelationTest
 */
abstract class AbstractCloudRelationTest extends QueryTest with SQLTestUtils
  with Eventually
  with HiveSingletonTrait
  with CloudSuiteTrait with BeforeAndAfterAll {

  import spark.implicits._

  val dataSourceName: String

  protected override def beforeAll(): Unit = {
    super.beforeAll()
//    COMMITTERS_BY_NAME(DIRECTORY).bind()


    // validate the conf by asserting that the spark conf is bonded
    // to the partitioned committer.
    val sparkConf = spark.conf
    assert(ObjectStoreConfigurations.PARQUET_COMMITTER_CLASS ===
        sparkConf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key),
      s"wrong value of ${SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS}")
    assert(ObjectStoreConfigurations.PATH_OUTPUT_COMMITTER_NAME ===
        sparkConf.get(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key),
      s"wrong value of ${SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS}")
  }

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
    SparkContext.getActive.getOrElse(fail("No active spark context"))
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

  lazy val partitionedTestDF1: DataFrame = (for {
    i <- 1 to 3
    p2 <- Seq("foo", "bar")
  } yield {
    (i, s"val_$i", 1, p2)
  }).toDF("a", "b", "p1", "p2")

  lazy val partitionedTestDF2: DataFrame = (for {
    i <- 1 to 3
    p2 <- Seq("foo", "bar")
  } yield {
    (i, s"val_$i", 2, p2)
  }).toDF("a", "b", "p1", "p2")

  protected lazy val partitionedTestDF: DataFrame =
    partitionedTestDF1.union(partitionedTestDF2)

  /**
   * Get
   * @return
   */
  protected def defaultOutputValidator(): Option[Path => Unit] = {
    Some(assertSuccessFileExists)
  }

  /**
   * Verify that the _SUCCESS file exists in a directory.
   * @param destDir destination dir
   */
  protected def assertSuccessFileExists(destDir: Path): Unit = {
    resolveSuccessFile(destDir, true)
  }

  /**
   * Get the success file
   * @param destDir destination of the query
   * @param requireS3ACommitter is an S3A committer required
   * @return the status
   */
  protected def resolveSuccessFile(destDir: Path, requireS3ACommitter: Boolean): FileStatus = {
    val fs = getFilesystem(destDir)
    val success = new Path(destDir, "_SUCCESS")
    val status = fs.getFileStatus(success)
    if (status.getLen == 0) {
      logInfo(s"Status file $success exists and is an empty files")
      assert(!requireS3ACommitter,
        s"The committer used to create file $success was not an S3A Committer")
    } else {
      logInfo(s"Status file $success is of size ${status.getLen}")
    }
    status
  }

  /**
   * Generates a temporary path without creating the actual file/directory, pass
   * it to the function, then cleanup with a deleteQuietly().
   * A validator function can be supplied to validate the data.
   *
   * @param name test name, used in path calculation
   * @param validator optional validator function. The default is that returned by
   *                  defaultOutputValidator.
   * @param fn function to evaluate
   */
  protected def withPath(name: String,
    validator: Option[Path => Unit] = defaultOutputValidator())
    (fn: Path => Unit): Path = {
    val d = path(name)
    filesystem.delete(d, true)
    withDefinedPath(d, fn, validator)
  }

  /**
   * Execute the function within the defined path, which is deleted afterwards.
   * @param dir directory
   * @param fn fun to invoke
   * @param validator validator to run after the operation
   * @param deleteAfter should the dir be deleted after?
   * @return the directory
   */
  private def withDefinedPath(dir: Path,
    fn: Path => Unit,
    validator: Option[Path => Unit],
    deleteAfter: Boolean = true): Path = {
    try {
      fn(dir)
      waitForCompletion()
      validator.foreach(p => p.apply(dir))
    } finally {
      // wait for all tasks to finish before deleting files
      waitForCompletion()
      if (deleteAfter) {
        deleteQuietly(dir)
      }
    }
    dir
  }

  /**
   * Waits for all tasks on all executors to be finished.
   * This is just `waitForTasksToFinish()` copied over for cross-spark
   * compatibility.
   */
  protected def waitForCompletion(): Unit = {
    eventually(timeout(10.seconds)) {
      assert(spark.sparkContext.statusTracker
        .getExecutorInfos.map(_.numRunningTasks()).sum == 0)
    }
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected def withTempPathDir(
    name: String,
    validator: Option[Path => Unit] = defaultOutputValidator(),
    deleteAfter: Boolean = true)
    (fn: Path => Unit): Path = {
    val dir = path(name)
    filesystem.delete(dir, true)
    filesystem.mkdirs(dir)
    withDefinedPath(dir, fn, validator, deleteAfter)
  }

  /**
   * Generates a temporary path without creating the actual file/directory, then pass it to `f`.
   * If a file/directory is created there by `f`, it will be deleted after `f` returns.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected override def withTempPath(fn: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try {
      fn(path)
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

  protected val parquetDataTypes: Seq[DataType] = Seq(
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

  protected val orcSupportedDataTypes: Seq[DataType] = Seq(
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



}




