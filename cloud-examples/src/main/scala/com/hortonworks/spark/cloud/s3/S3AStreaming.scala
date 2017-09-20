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

package com.hortonworks.spark.cloud.s3

import com.hortonworks.spark.cloud.operations.CloudStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

/**
 * An example/test for streaming with a source of S3.
 */
object S3AStreaming extends CloudStreaming with S3AExampleSetup
  with SequentialIOPolicy {

  /**
   * This is never executed; it's just here as the source of the example in the
   * documentation.
   */
  def streamingExample(): Unit = {
    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Milliseconds(1000))
    try {
      val lines = ssc.textFileStream("s3a://testbucket/incoming")
      val matches = lines.filter(_.endsWith("3"))
      matches.print()
      ssc.start()
      ssc.awaitTermination()
    } finally {
      ssc.stop(true)
    }
  }
}
