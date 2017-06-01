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

import java.net.URI

import com.hortonworks.spark.cloud.CloudSuite
import org.apache.hadoop.fs.FileSystem

/**
 * Trait for Azure tests. Because no Azure-specific API calls are made, this test suite
 * will compile against Hadoop versions which lack the hadoop-azure module. However, all
 * the tests will be skipped.
 */
trait AzureTestSetup extends CloudSuite {

  override def enabled: Boolean =  {
    getConf.getBoolean(AZURE_TESTS_ENABLED, false)
  }

  def initFS(): FileSystem = {
    val uri = new URI(requiredOption(AZURE_TEST_URI))
    logDebug(s"Executing Azure tests against $uri")
    createFilesystem(uri)
  }


}
