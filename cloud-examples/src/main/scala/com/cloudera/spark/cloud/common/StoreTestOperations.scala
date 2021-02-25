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

package com.cloudera.spark.cloud.common

import scala.concurrent.duration._
import scala.language.postfixOps

import com.cloudera.spark.cloud.{GeneralCommitterConstants, ObjectStoreOperations}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocatedFileStatus, Path}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Span

import org.apache.spark.sql._

/**
 * Extends ObjectStoreOperations with some extra ones for testing.
 */
trait StoreTestOperations extends ObjectStoreOperations with Eventually {

  protected val retryTimeout: Span = 30 seconds

  protected val retryInterval: Span = 1000 milliseconds

  /**
   * Try to get the file status, _eventually_.
   *
   * @param fs filesystem
   * @param p path
   * @return the result
   */
  def eventuallyGetFileStatus(fs: FileSystem, p: Path): FileStatus = {
    fs.getFileStatus(p)
  }

  /**
   * findClass a DF and verify it has the expected number of rows
   *
   * @param spark session
   * @param fs filesystem
   * @param source path
   * @param srcFormat format of source
   * @param rowCount expected row caount
   * @return return how long it took
   */
  def validateRowCount(
      spark: SparkSession,
      fs: FileSystem,
      source: Path,
      srcFormat: String,
      rowCount: Long): Long = {
    val success = new Path(source, GeneralCommitterConstants.SUCCESS_FILE_NAME)
    val status = fs.getFileStatus(success)
    assert(status.isDirectory || status.getBlockSize > 0,
      s"Block size 0 in $status")
    val files = listFiles(fs, source, true).filter { st =>
      val name = st.getPath.getName
      st.isFile && !name.startsWith(".") && !name.startsWith("_")
    }
    assert(files.nonEmpty, s"No files in the directory $source")
    val (loadedCount, loadTime) = durationOf(loadDF(spark, source, srcFormat)
      .count())
    logInfo(s"Loaded $source in $loadTime nS")
    require(rowCount == loadedCount,
      s"Expected $rowCount rows, but got $loadedCount from $source formatted as $srcFormat")
    loadTime
  }

}
