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

import java.net.URI

import com.hortonworks.spark.cloud.ObjectStoreExample
import com.hortonworks.spark.cloud.s3.S3AConstants._
import com.hortonworks.spark.cloud.s3.S3ExampleSetup
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

private[cloud] class ApacheConExample extends ObjectStoreExample with S3ExampleSetup {

  override def action(sparkConf: SparkConf,
      args: Array[String]): Int = {

    val landsatOrc = "s3a://hwdev-stevel-demo/landsatOrc"

    val source = landsatOrc
    val srcURI = new URI(source)
    val srcPath = new Path(srcURI)

    applyObjectStoreConfigurationOptions(sparkConf, true)
    hconf(sparkConf, READAHEAD_RANGE, "512K")
    val randomIO = true
    hconf(sparkConf, INPUT_FADVISE, if (randomIO) RANDOM_IO else NORMAL_IO)

    val spark = SparkSession
        .builder
        .appName("ApacheConExample")
        .config(sparkConf)
        .getOrCreate()


    try {
      val sc = spark.sparkContext
      val config = new Configuration(sc.hadoopConfiguration)
      val srcFS = FileSystem.get(srcURI, config)

      val sqlDF = duration("select") {
        spark.sql(s"SELECT id, acquisitionDate,cloudCover" +
            s" FROM orc.`${landsatOrc}` where cloudCover < 50")
      }
      val clouds = sqlDF
/*
      val clouds = duration("filter") {
        sqlDF.filter("cloudCover < 30")
      }
*/
      duration("show") {
        clouds.show()
      }

      // log any published filesystem state
      print(s"\nSource: FS: $srcFS\n")

    } finally {
      spark.stop()
    }
    0
  }

}
