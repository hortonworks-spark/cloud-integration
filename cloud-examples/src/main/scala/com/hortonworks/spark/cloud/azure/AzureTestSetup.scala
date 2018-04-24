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

package com.hortonworks.spark.cloud.azure

import java.io.{EOFException, FileNotFoundException}
import java.net.URI

import com.hortonworks.spark.cloud.common.CloudTestKeys._
import com.hortonworks.spark.cloud.common.{CloudSuite, CsvDatasourceSupport}
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Trait for Azure  ADL tests.
 *
 * This trait supports CSV data source by copying over the data from S3A if
 * it isn't already in a WASB URL
 */
trait AzureTestSetup extends CloudSuite with CsvDatasourceSupport {

  override def enabled: Boolean =  {
    getConf.getBoolean(AZURE_TESTS_ENABLED, false)
  }

  def initFS(): FileSystem = {
    val uri = new URI(requiredOption(AZURE_TEST_URI))
    logDebug(s"Executing Azure tests against $uri")
    createFilesystem(uri)
  }

  /**
   * This is the CSV test file on S3A. It is not the FS which is used
   */
  lazy val S3A_CSV_TESTFILE: Option[Path] = {
    val pathname = getConf.get(S3A_CSVFILE_PATH, S3A_CSV_PATH_DEFAULT)
    if (!pathname.isEmpty) Some(new Path(pathname)) else None
  }

  /**
   * Predicate to define whether or not there's a CSV file to work with.
   *
   * @return true if the S3A CSV test file is defined.
   */
  override def hasCSVTestFile(): Boolean = S3A_CSV_TESTFILE.isDefined

  /**
   * Path to the CSV file's original source
   *
   * @return a path
   */
  override def sourceCSVFilePath: Option[Path] = S3A_CSV_TESTFILE

  protected var testCSVFile: Option[Path] = None

  protected var deleteTestCSVFile = false

  /**
   * Path to the CSV file used in the tests themselves; may differ from
   * the original source
   *
   * @return path to test data: valid after `prepareTestCSVFile`.
   */
  override def testCSVFilePath: Option[Path] =  testCSVFile

  /**
   * Any operation to prepare the CSV file. After completion, returns
   * the path to the test CSV file.
   */
  override def prepareTestCSVFile(): Unit = {
    require(hasCSVTestFile(), "No CSV file")
    require(isFilesystemDefined, "Test FS is not defined; call initFS() first")
    // here the CSV file is copied over
    val source = sourceCSVFilePath.get
    if (source.toUri.getScheme == "wasb") {
      // source is already in Azure
      testCSVFile = sourceCSVFilePath
      deleteTestCSVFile = false
    } else {
      val srcStatus = source.getFileSystem(getConf).getFileStatus(source)
      if (srcStatus.getLen == 0) {
        throw new EOFException(s"File $source is an empty file")
      }
      // need to copy over
      val destFile = path(source.getName)
      testCSVFile = Some(destFile)
      var toCopy = false
      try {
        val status = filesystem.getFileStatus(destFile)
        if (status.getLen != srcStatus.getLen) {
          logInfo(s"Dest file exists, but length of $status != source data $srcStatus")
        } else {
          logInfo(s"Datafile exists; no copy needed: $status")
          toCopy = false
        }
      } catch {
        case _ : FileNotFoundException =>
          toCopy = true
      }
      if (toCopy) {
        copyFile(sourceCSVFilePath.get, destFile, getConf, true)
      }
      // todo? leave or delete?

    }
  }

}
