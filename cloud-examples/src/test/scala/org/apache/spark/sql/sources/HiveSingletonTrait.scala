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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * A trait for tests which bonds to a hive singleton.
 * After all tests the hive context is reset.
 */
trait HiveSingletonTrait extends SparkFunSuite with BeforeAndAfterAll {
  override protected val enableAutoThreadAudit = false
  protected val spark: SparkSession = HiveSingleton.sparkSession
  protected val hiveContext: TestHiveContext = HiveSingleton

  protected override def afterAll(): Unit = {
    try {
      hiveContext.reset()
    } finally {
      super.afterAll()
    }
  }

}

object HiveSingleton
  extends TestHiveContext(
    new SparkContext(
      System.getProperty("spark.sql.test.master", "local[1]"),
      "TestSQLContext",
      new SparkConf()
        .set("spark.sql.test", "")
        .set(SQLConf.CODEGEN_FALLBACK.key, "false")
        .set("spark.sql.hive.metastore.barrierPrefixes",
          "org.apache.spark.sql.hive.execution.PairSerDe")
        .set("spark.sql.warehouse.dir",
          TestSetup.makeWarehouseDir().toURI.getPath)
        .set("spark.ui.enabled", "false")
        .set("spark.unsafe.exceptionOnMemoryLeak", "true")
        .setAll(ObjectStoreConfigurations.RW_TEST_OPTIONS)
    )
  )


object TestSetup {

  def makeWarehouseDir(): File = {
    val warehouseDir = Utils.createTempDir(namePrefix = "warehouse")
    warehouseDir.delete()
    warehouseDir
  }
}
