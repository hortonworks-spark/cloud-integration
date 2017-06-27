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


import com.hortonworks.spark.cloud.common.CSVReadSuite

/**
 * A suite of tests reading in the S3A CSV file.
 */
class S3ACSVReadSuite extends CSVReadSuite with S3ATestSetup {

  init()

  def init(): Unit = {
    setupFilesystemConfiguration(getConf)
    if (enabled) {
      initDatasources()
    }
  }


/*  class RemoteOutputIterator[T](private val source: RemoteIterator[T]) extends Iterator[T] {
    def hasNext: Boolean = source.hasNext

    def next: T = source.next()
  }*/

  /**
   * This doesn't do much, except that it is designed to be pasted straight into
   * Zeppelin and work
   */
/*  ctest("DirOps", "simple directory ops in spark context process") {
    val source = CSV_TESTFILE.get
    sc = new SparkContext("local", "CSVgz", newSparkConf(source))

    import org.apache.hadoop.fs._
    val landsat = "s3a://landsat-pds/scene_list.gz"
    val landsatPath = new Path(landsat)
    val fs = landsatPath.getFileSystem(sc.hadoopConfiguration)
    val files = fs.listFiles(landsatPath.getParent, false)
    val listing = new RemoteOutputIterator(files)
    listing.foreach(print(_))

  }*/

}
