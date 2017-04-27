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

/**
 * Test constants
 */
class S3ATestConstants {
  /**
   * Various s3guard tests.
   */
  val TEST_S3GUARD_PREFIX = "fs.s3a.s3guard.test"
  val TEST_S3GUARD_ENABLED: String = TEST_S3GUARD_PREFIX + ".enabled"
  val TEST_S3GUARD_AUTHORITATIVE: String = TEST_S3GUARD_PREFIX +
    ".authoritative"
  val TEST_S3GUARD_IMPLEMENTATION: String = TEST_S3GUARD_PREFIX +
    ".implementation"
  val TEST_S3GUARD_IMPLEMENTATION_LOCAL = "local"
  val TEST_S3GUARD_IMPLEMENTATION_DYNAMO = "dynamo"
  val TEST_S3GUARD_IMPLEMENTATION_DYNAMODBLOCAL = "dynamodblocal"
  val TEST_S3GUARD_IMPLEMENTATION_NONE = "none"
}
