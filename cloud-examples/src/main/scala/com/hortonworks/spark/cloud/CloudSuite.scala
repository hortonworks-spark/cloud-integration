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

package com.hortonworks.spark.cloud

import java.io.{File, FileNotFoundException}

import com.hortonworks.spark.cloud.CloudTestKeys._
import com.hortonworks.spark.cloud.s3.S3ACommitterConstants._
import com.hortonworks.spark.cloud.s3.S3AConstants
import org.apache.hadoop.conf.Configuration
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.LocalSparkContext
import org.apache.spark.internal.Logging

/**
 * A cloud suite.
 * Adds automatic loading of a Hadoop configuration file with login credentials and
 * options to enable/disable tests, and a mechanism to conditionally declare tests
 * based on these details
 */
abstract class CloudSuite extends FunSuite
    with LocalSparkContext with BeforeAndAfter
    with Eventually with S3AConstants with CloudSuiteTrait {
}

object CloudSuite extends Logging with S3AConstants
  with CloudSuiteTrait {

  private var configLogged = false


  /**
   * Load the configuration file from the system property `SYSPROP_CLOUD_TEST_CONFIGURATION_FILE`.
   * Throws FileNotFoundException if a configuration is named but not present.
   * @return the configuration
   */
  def loadConfiguration(): Configuration = {
    val config = new Configuration(true)
    getKnownSysprop(SYSPROP_CLOUD_TEST_CONFIGURATION_FILE).foreach { filename =>
      logDebug(s"Configuration property = `$filename`")
      val f = new File(filename)
      if (f.exists()) {
        // unsynced but its only a log statement
        if (configLogged) {
          configLogged = true
          logInfo(s"Loading configuration from $f")
        }
        config.addResource(f.toURI.toURL)
      } else {
        throw new FileNotFoundException(s"No file '$filename'" +
          s" declared in property $SYSPROP_CLOUD_TEST_CONFIGURATION_FILE")
      }
    }
    overlayConfiguration(
      config,
      Seq(
        REQUIRED_HADOOP_VERSION,
        S3A_COMMITTER_TEST_ENABLED,
        S3GUARD_TEST_ENABLED,
        S3_CLIENT_FACTORY_IMPL,
        S3GUARD_IMPLEMENTATION,
        S3_METADATA_STORE_IMPL,
        METADATASTORE_AUTHORITATIVE,
        SCALE_TEST_ENABLED,
        SCALE_TEST_SIZE_FACTOR,
        S3A_ENCRYPTION_KEY_1,
        S3A_ENCRYPTION_KEY_2
      )
    )

    // setup the committer from any property passed in
    getKnownSysprop(S3A_COMMITTER_NAME).foreach(committer => {
      val binding = COMMITTERS_BY_NAME(committer.toLowerCase())
      binding.bind(config)
      logInfo(s"Using committer binding $binding")
    })
    config
  }

}
