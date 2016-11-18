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

import com.hortonworks.spark.cloud.ObjectStoreExample
import com.hortonworks.spark.cloud.s3.S3ExampleSetup
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Pull in Landsat CSV and convert to Parquet input; do a couple of operations
 * on that to round things out.
 *
 * See <a href="http://landsat.usgs.gov/tools_faq.php">Landsat Web Site</a>
 * for details about terminology and raw data.
 *
 * header:
 * {{{
 *   entityId: string : LC80101172015002LGN00
 *   acquisitionDate: timestamp: 2015-01-02 15:49:05.571384
 *   cloudCover: double: 80.81
 *   processingLevel: string: L1GT
 *   path: integer: 10
 *   row: integer 117
 *   min_lat: double: -79.09923
 *   min_lon: double: -139.6608
 *   max_lat: double: -77.7544
 *   max_lon: double: -125.09297
 *   download_url: string
 *     https://s3-us-west-2.amazonaws.com/landsat-pds/L8/010/117/LC80101172015002LGN00/index.html
 *
 * }}}
 */
private[cloud] class AzureStreamingExample extends ObjectStoreExample with S3ExampleSetup {

  /**
   * List of the command args for the current example.
   * @return a string
   */
  override protected def usageArgs(): String = {
    "<dest> <execute-seconds> <interval-seconds>"
  }

  /**
   * Action to execute.
   *
   * @param sparkConf configuration to use
   * @param args argument array
   * @return an exit code
   */
  override def action(
      sparkConf: SparkConf,
      args: Array[String]): Int = {
    if (args.length !=  3) {
      return usage()
    }
    sparkConf.setAppName("CloudStreaming")
    applyObjectStoreConfigurationOptions(sparkConf, false)
    val dest = args(0)
    val delay = Integer.valueOf(args(1))
    val interval = Integer.valueOf(args(2))

    // Create the context
    val streaming = new StreamingContext(sparkConf, Seconds(10))

    try {
      // Create the FileInputDStream on the directory regexp and use the
      // stream to look for a new file renamed into it
      val destPath = new Path(dest)
      val sc = streaming.sparkContext
      val hc = sc.hadoopConfiguration

      val fs = FileSystem.get(destPath.toUri, hc)
      fs.delete(destPath, true)
      fs.mkdirs(destPath)

      val sightings = sc.longAccumulator("sightings")

      print("===================================")
      print(s"Looking for text files under ${destPath}")
      print("===================================")

      val lines = streaming.textFileStream(dest)

      val matches = lines.map(line => {
        sightings.add(1)
        print(s"[${sightings.value}]: $line")
        line
      })

      // materialize the operation
      matches.print()

      // start the streaming
      streaming.start()

      // sleep a bit to get streaming up and running
      Thread.sleep(delay * 1000)
      print("===================================")
      print(s"Seen ${sightings.value} lines")
      0
    } finally {
      streaming.stop(true)
    }
  }

}

private[cloud] object AzureStreamingExample {

  def main(args: Array[String]) {
    new AzureStreamingExample().run(args)
  }
}

