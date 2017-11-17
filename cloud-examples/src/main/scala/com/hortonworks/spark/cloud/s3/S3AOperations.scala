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

import com.hortonworks.spark.cloud.StoreTestOperations
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.hadoop.fs.{FileSystem, StorageStatistics}

/**
 * General S3A operations against a filesystem.
 */
class S3AOperations(sourceFs: FileSystem)
  extends StoreTestOperations {

  /**
   * S3A Filesystem.
   */
  private val fs = sourceFs.asInstanceOf[S3AFileSystem]

  /**
   * Get a sorted list of the FS statistics.
   */
  def getStorageStatistics(): List[StorageStatistics.LongStatistic] = {
    getStorageStatistics(fs)
  }

}
