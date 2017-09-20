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

import org.apache.hadoop.fs.FileSystem

/**
 * Tests reading in the CSV file using sequential and Random IO.
 */
class SeekReadTests extends CloudSuiteWithCSVDatasource  {

  override def enabled: Boolean = super.enabled && hasCSVTestFile


  ctest("SeekReadFully",
      """Assess cost of seek and read operations.
        | When moving the cursor in an input stream, an HTTP connection may be closed and
        | then re-opened. This can be very expensive; tactics like streaming forwards instead
        | of seeking, and/or postponing movement until the following read ('lazy seek') try
        | to address this. Logging these operation times helps track performance.
        | This test also tries to catch out a regression, where a `close()` operation
        | is implemented through reading through the entire input stream. This is exhibited
        | in the time to `close()` while at offset 0 being `O(len(file))`.
        |
        | Note also the cost of `readFully()`; this method call is common inside libraries
        | like Orc and Parquet.""".stripMargin) {
    val (source, fs) = getCSVSourceAndFileSystem()
    FileSystem.clearStatistics
    val stats = FileSystem.getStatistics(fs.getScheme,
      fs.getClass)
    stats.reset()
    val storageStats = fs.getStorageStatistics
    storageStats.reset()
    val st = logDuration("stat") {
      fs.getFileStatus(source)
    }
    val in = logDuration("open") {
      fs.open(source)
    }
    def time[T](operation: String)(testFun: => T): T = {
      logInfo(s"")
      var r = logDuration(operation + s" [pos = ${in.getPos}]")(testFun)
      logInfo(s"  ${in.getWrappedStream}")
      r
    }

    val eof = st.getLen

    time("read()") {
      assert(-1 !== in.read())
    }
    time("seek(256)") {
      in.seek(256)
    }
    time("seek(256)") {
      in.seek(256)
    }
    time("seek(EOF-2)") {
      in.seek(eof - 2)
    }
    time("read()") {
      assert(-1 !== in.read())
    }

    def readFully(offset: Long, len: Int): Unit = {
      time(s"readFully($offset, byte[$len])") {
        val bytes = new Array[Byte](len)
        assert(-1 !== in.readFully(offset, bytes))
      }
    }
    readFully(1L, 1)
    readFully(1L, 256)
    readFully(eof - 350, 300)
    readFully(260L, 256)
    readFully(1024L, 256)
    readFully(1536L, 256)
    readFully(8192L, 1024)
    readFully(8192L + 1024 + 512, 1024)

    time("seek(getPos)") {
      in.seek(in.getPos())
    }
    time("read()") {
      assert(-1 !== in.read())
    }
    logDuration("close()") {
      in.close
    }
    logInfo(s"Statistics $stats")
    dumpFileSystemStatistics(storageStats)

  }

}
