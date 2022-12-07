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

package com.cloudera.spark.cloud.csv

import com.cloudera.spark.cloud.abfs.AbfsTestSetup
import com.cloudera.spark.cloud.ObjectStoreConfigurations.ABFS_READAHEAD_HADOOP_OPTIONS
import org.apache.hadoop.conf.Configuration

/**
 * The real test of HADOOP-18521.
 */
class AbfsHugeCsvIOSuite extends AbstractHugeCsvIOSuite with AbfsTestSetup() {

  init()

  /**
   * set up FS if enabled.
   */
  def init(): Unit = {
    if (enabled) {
      initFS()
    }
  }

  /**
   * Patch in ABFS readahead options, to ensure they are
   * always set.
   * @return the configuration to create the fs with
   */
  override def createConfiguration(): Configuration = {
    val conf = super.createConfiguration()
    for (kv <- ABFS_READAHEAD_HADOOP_OPTIONS) {
      conf.set(kv._1, kv._2)
    }
    conf
  }
}
