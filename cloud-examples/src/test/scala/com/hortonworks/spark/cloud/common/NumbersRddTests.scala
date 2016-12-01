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

import com.hortonworks.spark.cloud.CloudSuite
import org.apache.hadoop.fs.CommonConfigurationKeysPublic

import org.apache.spark.SparkContext

/**
 * Generate
 */
private[cloud] abstract class NumbersRddTests extends CloudSuite {

  after {
    cleanFilesystemInTeardown()
  }

  /**
   * cleanup is currently disabled
   */
  override protected def cleanFSInTeardownEnabled: Boolean = false;


  ctest("SaveRDD",
    """Generate an RDD and save it. No attempt is made to validate the output, so that
      | All post-test-setup FS IO which takes place is related to the committer.
    """.stripMargin) {
    sc = new SparkContext("local", "test", newSparkConf())
    val conf = sc.hadoopConfiguration
    assert(filesystemURI.toString === conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY))
    val entryCount = testEntryCount
    val numbers = sc.parallelize(1 to entryCount)
    val dest = testPath(filesystem, "numbers_rdd_tests")
    filesystem.delete(dest, true)
    logInfo(s"\nGenerating output under $dest\n")
    val lineLen = numbers.map(line => Integer.toHexString(line))
    numbers.saveAsTextFile(dest.toString)
    val fsInfo = filesystem.toString.replace("{", "\n{")
    logInfo(s"Filesystem statistics\n $fsInfo")
  }

}
