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

import com.hortonworks.spark.cloud.s3.S3AConstants

/**
 * The various test keys for the cloud tests.
 *
 * Different infrastructure tests may enabled/disabled.
 *
 * Timeouts and scale options are tuneable: this is important for remote test runs.
 *
 * All properties are set in the Java properties file referenced in the System property
 * `cloud.test.configuration.file`; this must be passed down by the test runner. If not set,
 * tests against live cloud infrastructures will be skipped.
 *
 * Important: Test configuration files containing cloud login credentials SHOULD NOT be saved to
 * any private SCM repository, and MUST NOT be saved into any public repository.
 * The best practise for this is: do not ever keep the keys in a directory which is part of
 * an SCM-managed source tree. If absolutely necessary, use a `.gitignore` or or equivalent
 * to ignore the files.
 *
 * It is possible to use XML XInclude references within a configuration file.
 * This allows for the credentials to be retained in a private location, while the rest of the
 * configuration can be managed under SCM:
 *
 *```
 *<configuration>
 *  <include xmlns="http://www.w3.org/2001/XInclude" href="/shared/security/auth-keys.xml"/>
 *</configuration>
 * ```
 */
object CloudTestKeys extends S3AConstants {

  /**
   * A system property which will be set on parallel test runs.
   */
  val SYSPROP_TEST_UNIQUE_FORK_ID = "test.unique.fork.id"

  /**
   * Name of the configuration file to load for test configuration.
   */
  val SYSPROP_CLOUD_TEST_CONFIGURATION_FILE = "cloud.test.configuration.file"

  /**
   * Prefix for scale tests.
   */
  val SCALE_TEST = "scale.test."

  /**
   * Option to declare whether or not a scale test is enabled
   */
  val SCALE_TEST_ENABLED = SCALE_TEST + "enabled"

  val SCALE_TEST_OPERATION_COUNT = SCALE_TEST + "operation.count"
  val SCALE_TEST_OPERATION_COUNT_DEFAULT = 10

  /**
   * Scale factor as a percentage of "default" load. Test runners may wish to scale
   * this down as well as up.
   */
  val SCALE_TEST_SIZE_FACTOR = SCALE_TEST + "size.factor"
  val SCALE_TEST_SIZE_FACTOR_DEFAULT = 100

  /**
   * Key defining the Amazon Web Services Account.
   */
  val AWS_ACCOUNT_ID = "fs.s3a.access.key"

  /**
   * Key defining the Amazon Web Services account secret.
   * This is the value which must be reset if it is ever leaked. The tests *must not* log
   * this to any output.
   */
  val AWS_ACCOUNT_SECRET = "fs.s3a.secret.key"

  /**
   * Are AWS tests enabled? If set, the user
   * must have AWS login credentials, defined via the environment
   * or in the XML test configuration file.
   */
  val S3A_TESTS_ENABLED = "s3a.tests.enabled"

  /**
   * A test bucket for S3A.
   * Data in this bucket under the `/test` directory will be deleted during test suite teardowns;
   */
  val S3A_TEST_URI = "s3a.test.uri"

  /**
   * Key referring to the csvfile. If unset, uses `S3A_CSV_PATH_DEFAULT`. If empty, tests
   * depending upon the CSV file will be skipped.
   */
  val S3A_CSVFILE_PATH = "s3a.test.csvfile.path"

  /**
   * Default source of a public multi-MB CSV file.
   * This is hosted by Amazon: it is the .csv.gz list of landsat scenes.
   * It is updated regularly with new scenes; the length and content of the list
   * cannot be predicted before test runs.
   */
  val S3A_CSV_PATH_DEFAULT = "s3a://landsat-pds/scene_list.gz"

  /**
   * Encryption key.
   */
  val S3A_ENCRYPTION_KEY_1 = "s3a.test.encryption.key.1"

  /**
   * Another encryption key.
   */
  val S3A_ENCRYPTION_KEY_2 = "s3a.test.encryption.key.2"

  /**
   * Key defining the Are Azure tests enabled? If set, the user
   * must have Azure login credentials, defined via the environment
   * or in the XML test configuration file.
   */
  val AZURE_TESTS_ENABLED = "azure.tests.enabled"

  /**
   * A test bucket for Azure.
   * Data in this bucket under the test directory will be deleted during test suite teardowns;
   */
  val AZURE_TEST_URI = "azure.test.uri"


  /**
   * Key defining the Are ADL tests enabled? If set, the user
   * must have ADL login credentials, defined via the environment
   * or in the XML test configuration file.
   */
  val ADL_TESTS_ENABLED = "adl.tests.enabled"

  /**
   * A test bucket for ADL.
   * Data in this bucket under the test directory will be deleted during test suite teardowns;
   */
  val ADL_TEST_URI = "adl.test.uri"


  /**
   * Are swift tests enabled?
   */
  val SWIFT_TESTS_ENABLED = "swift.tests.enabled"

  /**
   * A test bucket for Swift.
   * Data in this bucket under the `/test` directory will be deleted during test suite teardowns;
   */
  val SWIFT_TEST_URI = "swift.test.uri"

  /**
   * Name of a property for a required hadoop version; lets you verify that
   * the transitive hadoop versions is what you want.
   */
  val REQUIRED_HADOOP_VERSION = "required.hadoop.version"

  /**
   * Maven doesn't pass down empty properties as strings; it converts them to the string "null".
   * Here a special string is used to handle that scenario to make it clearer what's happening.
   */
  val UNSET_PROPERTY = "unset"

  /**
   * Various s3guard tests.
   */


  /**
   * Indicates that any committer tests are enabled
   */
  val S3A_COMMITTER_TEST_ENABLED = "s3a.committer.test.enabled"

  val S3GUARD_TEST_PREFIX = "s3a.s3guard.test"

  /**
   * Indicates that any s3guard tests are enabled
   */
  val S3GUARD_TEST_ENABLED = S3GUARD_TEST_PREFIX + ".enabled"

  val S3GUARD_TEST_AUTHORITATIVE = S3GUARD_TEST_PREFIX + ".authoritative"
  val S3GUARD_IMPLEMENTATION = S3GUARD_TEST_PREFIX + ".implementation"
  /** fs.s3a.s3guard.test.inconsistent */
  val S3GUARD_INCONSISTENT = S3GUARD_TEST_PREFIX + ".inconsistent"

  val S3GUARD_IMPLEMENTATION_LOCAL = "local"
  val S3GUARD_IMPLEMENTATION_DYNAMO = "dynamo"
  val S3GUARD_IMPLEMENTATION_DYNAMODBLOCAL = "localdynamodb"
  val S3GUARD_IMPLEMENTATION_NONE = "none"


}


