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

import com.hortonworks.spark.cloud.common.DataFrameTests

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

/**
 * Test Azure and DataFrames
 */
private[cloud] class AzureDataFrameSuite extends DataFrameTests with AzureTestSetup {

  init()

  def init(): Unit = {
    if (enabled) {
      initFS()
    }
  }

  /**
   * This is the source for the example; it is here to ensure it compiles.
   */
  def example(sparkConf: SparkConf): Unit = {
    val spark = SparkSession
        .builder
        .appName("DataFrames")
        .config(sparkConf)
        .getOrCreate()
    import spark.implicits._
    val numRows = 1000
    val sourceData = spark.range(0, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val dest = "wasb://yourcontainer@youraccount.blob.core.windows.net/dataframes"
    val orcFile = dest + "/data.orc"
    sourceData.write.format("orc").save(orcFile)
    // read it back
    val orcData = spark.read.format("orc").load(orcFile)
    // save it to parquet
    val parquetFile = dest + "/data.parquet"
    orcData.write.format("parquet").save(parquetFile)
    spark.stop()
  }
}
