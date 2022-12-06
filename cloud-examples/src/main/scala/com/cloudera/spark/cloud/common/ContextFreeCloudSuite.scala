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

import com.cloudera.spark.cloud.s3.S3AConstants
import org.scalatest.concurrent.Eventually
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.SparkSession

/**
 * A cloud suite which doesn't create a spark context.
 */
abstract class ContextFreeCloudSuite extends SparkFunSuite
  with BeforeAndAfter
  with Eventually with S3AConstants with CloudSuiteTrait {

}

/**
 * Cloud test suite with a spark session to clean up afterwards
 */
abstract class SparkSessionCloudSuite extends ContextFreeCloudSuite {

  var _sparkSession: SparkSession = null

  def sparkSession = _sparkSession

  def setSparkSession(s: SparkSession): Unit = {
    _sparkSession = s
  }

  /**
   * Close any spark session.
   */
  def closeSparkSession(): Unit = {
    if (_sparkSession != null) {
      _sparkSession.close()
      _sparkSession = null
      // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
      // (based on LocalSparkContext; no idea if still holds)
      System.clearProperty("spark.driver.port")
    }
  }


  override def afterEach(): Unit = {
    try {
      closeSparkSession()
    } finally {
      super.afterEach()
    }
  }

}