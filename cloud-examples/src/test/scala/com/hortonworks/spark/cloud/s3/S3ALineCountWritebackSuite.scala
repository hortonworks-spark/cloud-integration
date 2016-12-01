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

package com.hortonworks.spark.cloud.s3

import scala.concurrent.duration._
import scala.language.postfixOps

import com.hortonworks.spark.cloud.CloudSuite
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

/**
 * Test the `S3LineCount` entry point.
 */
private[cloud] class S3ALineCountWritebackSuite extends CloudSuite with S3ATestSetup {

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
    }
  }

  override def enabled: Boolean = super.enabled && hasCSVTestFile

  override def cleanFSInTeardownEnabled: Boolean = false

  after {
    cleanFilesystemInTeardown()
  }

  ctest("S3ALineCountWriteback",
    "Execute the S3ALineCount example with the results written back to the test filesystem.",
    !testAndCSVEndpointsDifferent(conf)) {
    val sourceFile = CSV_TESTFILE.get
    val sourceFS = FileSystem.get(sourceFile.toUri, conf)
    val sourceInfo = sourceFS.getFileStatus(sourceFile)
    val sparkConf = newSparkConf()
    sparkConf.setAppName("S3LineCount")
    val destDir = testPath(filesystem, "s3alinecount")
    assert(0 === S3ALineCount.action(sparkConf,
      Array(sourceFile.toString, destDir.toString)))


    val status = filesystem.getFileStatus(destDir)
    assert(status.isDirectory, s"Not a directory: $status")

    // only a small fraction of the source data is needed
    val expectedLen = sourceInfo.getLen / 1024

    def validateChildSize(qualifier: String, files: Seq[FileStatus]) = {
      val (filenames, size) = enumFileSize(destDir, files)
      logInfo(s"total size of $qualifier = $size bytes from ${files.length} files: $filenames")
      assert(size >= expectedLen, s"$qualifier size $size in files $filenames" +
          s" smaller than exoected length $expectedLen")
    }

    val stdInterval = interval(100 milliseconds)
    val appId = eventually(timeout(20 seconds), stdInterval) {
      validateChildSize("descendants",
        listFiles(filesystem, destDir, true)
            .filter(f => f.getPath.getName != "_SUCCESS"))

      validateChildSize("children",
        filesystem.listStatus(destDir,
          pathFilter(p => p.getName != "_SUCCESS")).toSeq)
    }
  }

  private def enumFileSize(destDir: Path, files: Seq[FileStatus]): (String, Long)  = {
    assert(files.nonEmpty, s"No files in destination directory $destDir")
    var size = 0L
    val filenames = new StringBuffer()
    files.foreach { f =>
      size += f.getLen
      filenames.append(" ").append(f.getPath)
    }
    (filenames.toString, size)
  }

}
