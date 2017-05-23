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
trait CloudTestKeys {

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
   * Defines the endpoint for the CSV test file.
   *
   * If not set, or set to "", the test endpoint remains that used by
   * the rest of the system.
   *
   * This property is needed to allow a public test dataset to be read off a
   * different S3 endpoint than the one used for the private read/write tests.
   */
  val S3A_CSVFILE_ENDPOINT = "s3a.test.csvfile.endpoint"

  /**
   * The default value: "s3.amazonaws.com". Amazon's US-east S3 endpoint, which will
   * actually handle V2 API requests against any S3 instance.
   */
  val S3A_CSVFILE_ENDPOINT_DEFAULT = "s3.amazonaws.com"

  /**
   * Key defining the Are AWS tests enabled? If set, the user
   * must have AWS login credentials, defined via the environment
   * or in the XML test configuration file.
   */
  val AZURE_TESTS_ENABLED = "azure.tests.enabled"

  /**
   * A test bucket for Azure.
   * Data in this bucket under the test directory will be deleted during test suite teardowns;
   */
  val AZURE_TEST_URI = "azure.test.uri"

  /**
   * Are swift tests enabled?
   */
  val SWIFT_TESTS_ENABLED = "swift.tests.enabled"

  /**
   * A test bucket for Swift.
   * Data in this bucket under the `/test` directory will be deleted during test suite teardowns;
   */
  val SWIFT_TEST_URI = "swift.test.uri"

  val MR_ALGORITHM_VERSION = "mapreduce.fileoutputcommitter.algorithm.version"
  val MR_COMMITTER_CLEANUPFAILURES_IGNORED = "mapreduce.fileoutputcommitter.cleanup-failures.ignored"

  val SUCCESSFUL_JOB_OUTPUT_DIR_MARKER = "mapreduce.fileoutputcommitter.marksuccessfuljobs"
  val FILEOUTPUTCOMMITTER_ALGORITHM_VERSION = "mapreduce.fileoutputcommitter.algorithm.version"
  val FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT = 2
  // Skip cleanup _temporary folders under job's output directory
  val FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED = "mapreduce.fileoutputcommitter.cleanup.skipped"

  /**
   * Indicates that any s3guard tests are enabled
   */
  val S3GUARD_TEST_ENABLED = "fs.s3a.s3guard.test.enabled"

  /**
   * Indicates that any committer tests are enabled
   */
  val S3A_COMMITTER_TEST_ENABLED = "fs.s3a.committer.test.enabled"

  /**
   * This is the "Pending" directory of the FileOutputCommitter;
   * data written here is, in that algorithm, renamed into place.
   * Value: {@value }.
   */
  val PENDING_DIR_NAME = "_temporary"
  /**
   * Marker file to create on success.
   */
  val SUCCESS_FILE_NAME = "_SUCCESS"

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

}
