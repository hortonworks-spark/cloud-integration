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

package com.cloudera.spark.cloud.local

import com.cloudera.spark.cloud.common.CloudSuiteTrait
import org.apache.hadoop.fs.FileSystem

/**
 * Trait for the local fs; goal is for benchmarking/validating/writing
 * new tests.
 *
 */
trait LocalTestSetup extends CloudSuiteTrait {

  override def enabled: Boolean = {
    getConf.getBoolean(GS_TESTS_ENABLED, false)
  }

  def initFS(): FileSystem = {
    val fs = getLocalFS
    setFilesystem(fs)
    fs
  }

  override def dynamicPartitioning: Boolean = true;
}
