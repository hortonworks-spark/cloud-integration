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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait CsvDatasourceSupport {

  /**
   * Predicate to define whether or not there's a CSV file to work with.
   *
   * @return true if the CSV test file is defined.
   */
  def hasCSVTestFile(): Boolean = false

  /**
   * Path to the CSV file's original source
   * @return a path
   */
  def sourceCSVFilePath : Option[Path] = None

  /**
   * Path to the CSV file used in the tests themselves; may differ from
   * the original source
   *
   * @return path to test data: valid after `prepareTestCSVFile`.
   */
  def testCSVFilePath : Option[Path] = sourceCSVFilePath

  /**
   * Get the test CSV file or raise an exception.
   * @return the CSV path for tests
   */
  def getTestCSVPath(): Path = testCSVFilePath.get

  /**
   * Any operation to prepare the CSV file. After completion, returns
   * the path to the test CSV file.
   */
  def prepareTestCSVFile(): Unit = {
    require(hasCSVTestFile(), "No CSV file")
    require(sourceCSVFilePath.isDefined, "No source CSV file")
  }

}
