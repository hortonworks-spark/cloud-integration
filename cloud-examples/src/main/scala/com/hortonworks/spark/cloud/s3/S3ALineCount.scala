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

import com.hortonworks.spark.cloud.operations.LineCount
import com.hortonworks.spark.cloud.CloudTestKeys._

import org.apache.spark.SparkConf

/**
 * A line count example which has a default reference of a public Amazon S3
 * CSV .gz file in the absence of anything on the command line.
 */
object S3ALineCount extends LineCount with S3AExampleSetup with SequentialIOPolicy {

  override def defaultSource: Option[String] = {
    Some(S3A_CSV_PATH_DEFAULT)
  }

  override def maybeEnableAnonymousAccess(
      sparkConf: SparkConf,
      dest: Option[String]): Unit = {
    if (dest.isEmpty) {
      hconf(sparkConf, AWS_CREDENTIALS_PROVIDER, ANONYMOUS_CREDENTIALS)
    }
  }
}
