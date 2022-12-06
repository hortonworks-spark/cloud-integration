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

package com.cloudera.spark.cloud.csv

import com.cloudera.spark.cloud.local.LocalTestSetup
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext

class LocalHugeCsvIOSuite extends AbstractHugeCsvIOSuite with LocalTestSetup {

  init()

  /**
   * set up FS if enabled.
   */
  def init(): Unit = {
    initFS()
  }

  def rowCount = 1024 * 1024

  ctest("csv-io",
    "Use SparkContext.saveAsNewAPIHadoopFile() to save data to a file") {
    sc = new SparkContext("local", "test", newSparkConf())
    val numbers = sc.parallelize(1 to testEntryCount)
    val basePath = testPath(filesystem, "base")
    val avroPath = new Path(basePath, "avro")
    val csvio = new CsvIO(sc., sc.hadoopConfiguration, rowCount)
    saveAsNewTextFile(numbers, basePath, sc.hadoopConfiguration)
    val basePathStatus = filesystem.getFileStatus(basePath)
    // check blocksize in file status
    logDuration("listLeafDir") {
      listLeafDirStatuses(filesystem, basePathStatus)
    }
    val (leafFileStatus, _) = durationOf {
      listLeafStatuses(filesystem, basePathStatus)
    }
    // files are either empty or have a block size
    leafFileStatus.foreach(s => assert(s.getLen == 0 || s.getBlockSize > 0))

    // and run binary files over it to see if SPARK-6527 is real or not.
    sc.binaryFiles(basePath.toUri.toString, 1).map {
      _ => 1
    }.count()
  }
}
