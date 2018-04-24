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

package org.apache.spark.sql.sources

import java.io.File

import com.hortonworks.spark.cloud.ObjectStoreConfigurations
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * A trait for tests which bonds to a hive context
 * After all tests the hive context is reset then it and the spark session
 * closed.
 */
trait HiveTestTrait extends SparkFunSuite with BeforeAndAfterAll {
//  override protected val enableAutoThreadAudit = false
  protected var hiveContext: HiveInstanceForTests = _
  protected var spark: SparkSession = _


  protected override def beforeAll(): Unit = {
    super.beforeAll()
    // set up spark and hive context
    hiveContext = new HiveInstanceForTests()
    spark = hiveContext.sparkSession
  }

  protected override def afterAll(): Unit = {
    try {
      SparkSession.clearActiveSession()

      if (hiveContext != null) {
        hiveContext.reset()
        hiveContext = null
      }
      if (spark != null) {
        spark.close()
        spark = null
      }
    } finally {
      super.afterAll()
    }
  }

}

class HiveInstanceForTests
  extends TestHiveContext(
    new SparkContext(
      System.getProperty("spark.sql.test.master", "local[1]"),
      "TestSQLContext",
      new SparkConf()
        .setAll(ObjectStoreConfigurations.RW_TEST_OPTIONS)
        .set("spark.sql.warehouse.dir",
          TestSetup.makeWarehouseDir().toURI.getPath)
    )
  ) {

}




object TestSetup {

  def makeWarehouseDir(): File = {
    val warehouseDir = Utils.createTempDir(namePrefix = "warehouse")
    warehouseDir.delete()
    warehouseDir
  }
}
