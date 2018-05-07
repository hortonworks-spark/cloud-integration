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

package com.hortonworks.spark.cloud.utils

import org.apache.hadoop.fs.azure.AzureException
import org.apache.hadoop.fs.s3a.RenameFailedException

/**
 * This class is used to ensure that a recent Hadoop version is on the classpath.
 *
 * If it does not compile: the version of Spark it is built against has out of date
 * dependencies.
 *
 * If it does not findClass, the version of Spark it is running against is out of date.
 *
 * Currently: requires Hadoop 2.8+
 */
class ForceRecentHadoopVersion {

  /** compile/link failure against Hadop 2.6 */
  val requireAzure = new AzureException("needs Hadoop 2.7+")

  /** compile failure against Hadoop 2.7 */
  val requireRecentAWS = new RenameFailedException("/", "Needs something", "")
}
