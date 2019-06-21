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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Any cloud suite which requires the datasource to be a (possibly copied over)
 * CSV file.
 */
class CloudSuiteWithCSVDatasource extends CloudSuite with CsvDatasourceSupport {

  /**
   * Call this to set up the datasource for tests.
   */
  def initDatasources(): Unit = {
    if (hasCSVTestFile()) {
      prepareTestCSVFile()
      testCSVFilePath.get
    }
  }

  /**
   * Get the CSV source path and filesystem to read from it.
   * The filesystem uses the endpoint defined for the CSV file.
   *
   * @return Patn and FS of a CSV source file.
   */
  def getCSVSourceAndFileSystem(): (Path, FileSystem) = {
    val source = getTestCSVPath()
    (source, FileSystem.newInstance(source.toUri, new Configuration(getConf)))
  }
}
