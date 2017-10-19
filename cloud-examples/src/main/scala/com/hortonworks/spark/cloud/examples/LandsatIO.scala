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

package com.hortonworks.spark.cloud.examples

import com.hortonworks.spark.cloud._
import com.hortonworks.spark.cloud.s3._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.hortonworks.spark.cloud.CloudTestKeys._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
/**
 * Reusable landsat assist.
 */
object LandsatIO {

  /**
   * Take a dataframe and add the landsat columns
   *
   * @param csv
   * @return
   */
  def addLandsatColumns(csv: DataFrame): DataFrame = {
    csv
      .withColumnRenamed("entityId", "id")
      .withColumn("acquisitionDate",
        csv.col("acquisitionDate").cast(TimestampType))
      .withColumn("cloudCover", csv.col("cloudCover").cast(DoubleType))
      .withColumn("path", csv.col("path").cast(IntegerType))
      .withColumn("row", csv.col("row").cast(IntegerType))
      .withColumn("min_lat", csv.col("min_lat").cast(DoubleType))
      .withColumn("min_lon", csv.col("min_lon").cast(DoubleType))
      .withColumn("max_lat", csv.col("max_lat").cast(DoubleType))
      .withColumn("max_lon", csv.col("max_lon").cast(DoubleType))

  }

  /**
   * CSV parsing options: does not include type inference.
   */
  val csvOptions = Map(
    "header" -> "true",
    "ignoreLeadingWhiteSpace" -> "true",
    "ignoreTrailingWhiteSpace" -> "true",
    "timestampFormat" -> "yyyy-MM-dd HH:mm:ss.SSSZZZ",
    "mode" -> "FAILFAST"
  )
}

case class LandsatImage(
    id: StringType,
    acquisitionDate: TimestampType,
    cloudCover: DoubleType,
    path: IntegerType,
    row: IntegerType,
    min_lat: IntegerType,
    min_lon: IntegerType,
    max_lat: IntegerType,
    max_lon: IntegerType)
