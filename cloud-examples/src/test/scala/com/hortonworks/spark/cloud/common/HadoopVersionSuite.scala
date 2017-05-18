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

package com.hortonworks.spark.cloud.common

import scala.collection.immutable._

import org.scalatest.Matchers
import com.hortonworks.spark.cloud.CloudSuite._
import org.apache.hadoop.util.VersionInfo
import org.junit.Assume

import org.apache.spark.SparkFunSuite

class HadoopVersionSuite extends SparkFunSuite with Matchers {

  test("Check Hadoop version") {
    val configuration = getAnyConfiguration

    overlayConfiguration(
      configuration,
      Seq(REQUIRED_HADOOP_VERSION)
    )

    logInfo( s"""
       | build = ${VersionInfo.getBuildVersion}
       | build date = ${VersionInfo.getDate}
       | built by ${VersionInfo.getUser}
    """.stripMargin)
    val v = configuration.get(REQUIRED_HADOOP_VERSION, UNSET_PROPERTY)
    Assume.assumeTrue(v != UNSET_PROPERTY)
    assert(v == VersionInfo.getVersion())
  }
}
