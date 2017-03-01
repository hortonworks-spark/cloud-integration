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
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Simple example of streaming on Azure.
 */
class AzureStreamingExample extends ObjectStoreExample {

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

 object AzureStreamingExample {

  def main(args: Array[String]) {
    new AzureStreamingExample().run(args)
  }
}

