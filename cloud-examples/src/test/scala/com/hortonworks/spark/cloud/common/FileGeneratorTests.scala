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

package com.hortonworks.spark.cloud.common

import com.hortonworks.spark.cloud.{CloudFileGenerator, CloudSuite}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf

/**
 * Test the `FileGenerator` entry point. Use a small file number to keep the unit tests fast; some
 * cloud infras are very slow here. System tests can use the CLI instead.
 */
abstract class FileGeneratorTests extends CloudSuite {

  ctest("FileGenerator", "Execute the FileGenerator example") {
    val conf = newSparkConf()
    conf.setAppName("FileGenerator")
    val destDir = testPath(filesystem, "filegenerator")
    val months = 2
    val fileCount = 1
    val rowCount = 500

    assert(0 === generate(conf, destDir, months, fileCount, rowCount))

    val status = filesystem.getFileStatus(destDir)
    assert(status.isDirectory, s"Not a directory: $status")

    val totalExpectedFiles = months * fileCount

    // do a recursive listFiles
    val listing = duration("listFiles(recursive)") {
      listFiles(filesystem, destDir, true)
    }
    var recursivelyListedFilesDataset = 0L
    var recursivelyListedFiles = 0
    duration("scan result list") {
      listing.foreach { status =>
        recursivelyListedFiles += 1
        recursivelyListedFilesDataset += status.getLen
        logInfo(s"${status.getPath}[${status.getLen}]")
      }
    }

    logInfo(s"FileSystem $filesystem")
    assert(totalExpectedFiles === recursivelyListedFiles)
  }

  /**
   * Generate a set of files
   * @param conf configuration
   * @param destDir destination directory
   * @param monthCount number of months to generate
   * @param fileCount files per month
   * @param rowCount rows per file
   * @return the exit code of the operation
   */
  def generate(
      conf: SparkConf,
      destDir: Path,
      monthCount: Int,
      fileCount: Int,
      rowCount: Int): Int = {
    val result = new CloudFileGenerator().action(
      conf,
      Seq(destDir,
        monthCount,
        fileCount,
        rowCount))
    result
  }
}
