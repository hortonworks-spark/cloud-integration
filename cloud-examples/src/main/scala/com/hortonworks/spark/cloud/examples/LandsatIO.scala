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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
/**
 * Reusable landsat assist.
 * s3a://landsat-pds/scene_list.gz
 * http://landsat-pds.s3.aws.com/scene_list.gz
 * 
 * entityId,acquisitionDate,cloudCover,processingLevel,path,row,min_lat,min_lon,max_lat,max_lon,download_url
 * LC80101172015002LGN00,2015-01-02 15:49:05.571384,80.81,L1GT,10,117,-79.09923,-139.66082,-77.7544,-125.09297,https://s3-us-west-2.amazonaws.com/landsat-pds/L8/010/117/LC80101172015002LGN00/index.html
 * LC80260392015002LGN00,2015-01-02 16:56:51.399666,90.84,L1GT,26,39,29.23106,-97.48576,31.36421,-95.16029,https://s3-us-west-2.amazonaws.com/landsat-pds/L8/026/039/LC80260392015002LGN00/index.html
 * LC82270742015002LGN00,2015-01-02 13:53:02.047000,83.44,L1GT,227,74,-21.28598,-59.27736,-19.17398,-57.07423,https://s3-us-west-2.amazonaws.com/landsat-pds/L8/227/074/LC82270742015002LGN00/index.html
 * LC82270732015002LGN00,2015-01-02 13:52:38.110317,52.29,L1T,227,73,-19.84365,-58.93258,-17.73324,-56.74692,https://s3-us-west-2.amazonaws.com/landsat-pds/L8/227/073/LC82270732015002LGN00/index.html
 * LC82270622015002LGN00,2015-01-02 13:48:14.888002,38.85,L1T,227,62,-3.95294,-55.38896,-1.84491,-53.32906,https://s3-us-west-2.amazonaws.com/landsat-pds/L8/227/062/LC82270622015002LGN00/index.html
 * LC82111152015002LGN00,2015-01-02 12:30:31.546373,22.93,L1GT,211,115,-78.54179,-79.36148,-75.51003,-69.81645,https://s3-us-west-2.amazonaws.com/landsat-pds/L8/211/115/LC82111152015002LGN00/index.html
 *
 */
object LandsatIO {

  /**
   * Take a dataframe and add the landsat columns, including type
   * casting and adding year/month/day fields derived from timestamps
   *
   * @param csv source CSV
   * @return dataframe with typed columns
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
      .withColumn("year",
        year(col("acquisitionDate")))
      .withColumn("month",
        month(col("acquisitionDate")))
      .withColumn("day",
        month(col("acquisitionDate")))
  }

  /**
   * CSV parsing options: does not include type inference.
   */
  val CsvOptions = Map(
    "header" -> "true",
    "ignoreLeadingWhiteSpace" -> "true",
    "ignoreTrailingWhiteSpace" -> "true",
    "timestampFormat" -> "yyyy-MM-dd HH:mm:ss.SSSZZZ",
    "inferSchema" -> "false",
    "multiLine" -> "false"
  )

  /**
   * Headers in order
   */
  val Headers = Seq(
    "entityId",
    "acquisitionDate",
    "cloudCover",
    "processingLevel",
    "path",
    "row",
    "min_lat",
    "min_lon",
    "max_lat",
    "max_lon",
    "download_url")

  /**
   * The struct type of the CSV data.
   */
  def buildCsvSchema(): StructType = {
    var st = new StructType()
    Headers.foreach(h => {
      st = st.add(h, StringType)
    })
    st
  }

}

case class LandsatImage(
    id: String,
    acquisitionDate: Long,
    year: Int,
    month: Int,
    day: Int,
    cloudCover: Double,
    path: Int,
    row: Int,
    min_lat: Double,
    min_lon: Double,
    max_lat: Double,
    max_lon: Double)

