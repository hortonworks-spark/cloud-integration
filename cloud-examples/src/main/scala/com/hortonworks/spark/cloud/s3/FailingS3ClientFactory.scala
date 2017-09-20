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

import java.io.IOException

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.AmazonS3
import org.apache.hadoop.fs.s3a.{DefaultS3ClientFactory, InconsistentAmazonS3Client}

import org.apache.spark.internal.Logging

/**
 * This is here to verify that factory settings make their way down
 * to queries; can be enabled from build profiles
 */
class FailingS3ClientFactory extends DefaultS3ClientFactory  with Logging {

  logWarning("Failing S3 Client instantiated")

  override protected def newAmazonS3Client(
      credentials: AWSCredentialsProvider,
      awsConf: ClientConfiguration): AmazonS3 = {
    throw new IOException("failing")
  }

}

