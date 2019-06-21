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
 * Extends ObjectStoreOperations with some extra ones for testing: essentially
 * handling for eventually consistent filesystems through retries.
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
    eventually(timeout(retryTimeout),
      interval(retryInterval)) {
      fs.getFileStatus(p)
    }
  }

  /**
   * Try to get the directory listing, _eventually_.
   *
   * @param fs filesystem
   * @param p path
   * @return the result
   */
  def eventuallyListStatus(fs: FileSystem, p: Path): Array[FileStatus] = {
    eventually(timeout(retryTimeout),
      interval(retryInterval)) {
      fs.listStatus(p)
    }
  }

  /**
   * Perform a recursive the directory listing, _eventually_.
   * It does not wait for the directory tree to become consistent,
   * only for the root path to be listable.
   *
   * @param fs filesystem
   * @param p path
   * @return the result
   */
  def eventuallyListFiles(fs: FileSystem, p: Path): Seq[LocatedFileStatus] = {
    eventually(timeout(retryTimeout),
      interval(retryInterval)) {
      listFiles(fs, p, true)
    }
  }

  //
  // return how long it took
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
    waitForConsistency(fs)
    val success = new Path(source, GeneralCommitterConstants.SUCCESS_FILE_NAME)
    val status = eventuallyGetFileStatus(fs, success)
    assert(status.isDirectory || status.getBlockSize > 0,
      s"Block size 0 in $status")
    val files = eventuallyListFiles(fs, source).filter { st =>
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

  /**
   * Any delay for consistency
   *
   * @return delay in millis; 0 is default.
   */
  def consistencyDelay(c: Configuration): Int = 0

  /**
   * Wait for the FS to be consistent.
   * If there is no inconsistency, this is a no-op
   */
  def waitForConsistency(fs: FileSystem): Unit = {
    waitForConsistency(fs.getConf)
  }

  /**
   * Wait for the FS to be consistent.
   * If there is no inconsistency, this is a no-op
   */
  def waitForConsistency(c: Configuration): Unit = {
    val delay = consistencyDelay(c)
    if (delay > 0) {
      Thread.sleep(delay * 2)
    }
  }

  /**
   * Recursive delete. Special feature: waits for the inconsistency delay
   * both before and after if the fs property has it set to anything
   *
   * @param fs
   * @param path
   * @return
   */
  protected override def rm(
      fs: FileSystem,
      path: Path): Boolean = {
    waitForConsistency(fs)
    val r = super.rm(fs, path)
    waitForConsistency(fs)
    r
  }
}
