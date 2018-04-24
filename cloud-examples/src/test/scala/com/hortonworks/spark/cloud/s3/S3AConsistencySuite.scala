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

import com.hortonworks.spark.cloud.common.CloudSuite
import org.apache.hadoop.fs.Path

/**
 * Handle consistency with tests which require list consistency
 */
class S3AConsistencySuite extends CloudSuite with S3ATestSetup {

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
    }
  }

  after {
    cleanFilesystemInTeardown()
  }

  ctest("mkdir, mkfile",
    "Create a dir, a file, read the file", true) {
    val fs = filesystem
    val dir = testPath(fs, "mkfile")
    fs.mkdirs(dir)
    val file = new Path(dir, "file.txt")
    fs.delete(file, false)
    val fd = fs.create(file, false)
    fd.writeChars("hello")
    fd.close()
    val files = eventuallyListStatus(fs, dir)
    require(1 == files.length)
    val in = fs.open(file)
    in.seek(4)
    val b = new Array[Byte](3)
    in.readFully(2, b)
    in.close()
  }

  ctest("create & list",
    "create a file, list it", true) {
    val fs = filesystem
    val dir = testPath(fs, "create_and_list")
    val file = new Path(dir, "file.txt")
    val fd = fs.create(file, false)
    fd.writeChars("hello")
    fd.close()
    val files = eventuallyListStatus(fs, dir)
    require(1 === files.length)
    require(file === files(0).getPath)
  }

  ctest("rename",
    "Test the classic commit-by-rename algorithm ") {
    val fs = filesystem
    val work = testPath(fs, "work")
    fs.delete(work, true)
    val task00 = new Path(work, "task00")

    fs.mkdirs(task00)
    val out = fs.create(new Path(task00, "part-00"), true)
    out.writeChars("hello")
    out.close()
    // iterates to success. If this was a simple list this test would fail
    //val listing = eventuallyListStatus(fs, task00)
    val listing = fs.listStatus(task00)
    describe("Renaming")
    listing.foreach(stat =>
      fs.rename(stat.getPath, work)
    )
    describe(s"Filesystem state after rename: $fs")
    val statuses = fs.listStatus(work).filter(_.isFile)
    require("part-00" === statuses(0).getPath.getName)
  }

  /*
   * This test is here for slides
   */
/*
  ctest("example-for-slides", " ", false) {
import org.apache.hadoop.conf.Configuration

    val work = new Path("s3a://stevel-frankfurt/work")
    val fs = work.getFileSystem(new Configuration())
    val task00 = new Path(work, "task00")
    fs.mkdirs(task00)
    val out = fs.create(new Path(task00, "part-00"), false)
    out.writeChars("hello")
    out.close()
    waitForConsistency(fs)

    fs.listStatus(task00).foreach(stat =>
      fs.rename(stat.getPath, work)
    )
    val statuses = fs.listStatus(work).filter(_.isFile)
    require("part-00" == statuses(0).getPath.getName)
  }
*/

}
