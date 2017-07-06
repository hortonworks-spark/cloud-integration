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
 * S3A constants. Different Hadoop versions have an incomplete set of these; keeping them
 * in source here ensures that there are no compile/link problems.
 */
trait S3AConstants {
  val ACCESS_KEY = "fs.s3a.access.key"
  val SECRET_KEY = "fs.s3a.secret.key"
  val AWS_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider"
  val ANONYMOUS_CREDENTIALS = "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
  val SESSION_TOKEN = "fs.s3a.session.token"
  val MAXIMUM_CONNECTIONS = "fs.s3a.connection.maximum"
  val SECURE_CONNECTIONS = "fs.s3a.connection.ssl.enabled"
  val ENDPOINT = "fs.s3a.endpoint"
  val PATH_STYLE_ACCESS = "fs.s3a.path.style.access"
  val PROXY_HOST = "fs.s3a.proxy.host"
  val PROXY_PORT = "fs.s3a.proxy.port"
  val PROXY_USERNAME = "fs.s3a.proxy.username"
  val PROXY_PASSWORD = "fs.s3a.proxy.password"
  val PROXY_DOMAIN = "fs.s3a.proxy.domain"
  val PROXY_WORKSTATION = "fs.s3a.proxy.workstation"
  val MAX_ERROR_RETRIES = "fs.s3a.attempts.maximum"
  val ESTABLISH_TIMEOUT = "fs.s3a.connection.establish.timeout"
  val SOCKET_TIMEOUT = "fs.s3a.connection.timeout"
  val MAX_PAGING_KEYS = "fs.s3a.paging.maximum"
  val MAX_THREADS = "fs.s3a.threads.max"
  val KEEPALIVE_TIME = "fs.s3a.threads.keepalivetime"
  val MAX_TOTAL_TASKS = "fs.s3a.max.total.tasks"
  val MULTIPART_SIZE = "fs.s3a.multipart.size"
  val MIN_PERMITTED_MULTIPART_SIZE: Int = 5 * (1024 * 1024)
  val MIN_MULTIPART_THRESHOLD = "fs.s3a.multipart.threshold"
  val ENABLE_MULTI_DELETE = "fs.s3a.multiobjectdelete.enable"
  val BUFFER_DIR = "fs.s3a.buffer.dir"
  val FAST_UPLOAD = "fs.s3a.fast.upload"
  val FAST_BUFFER_SIZE = "fs.s3a.fast.buffer.size"
  val PURGE_EXISTING_MULTIPART = "fs.s3a.multipart.purge"
  val PURGE_EXISTING_MULTIPART_AGE = "fs.s3a.multipart.purge.age"
  val SERVER_SIDE_ENCRYPTION_ALGORITHM = "fs.s3a.server-side-encryption-algorithm"
  val SERVER_SIDE_ENCRYPTION_AES256 = "AES256"
  val SIGNING_ALGORITHM = "fs.s3a.signing-algorithm"
  val BLOCK_SIZE = "fs.s3a.block.size"
  val FS_S3A = "s3a"
  val USER_AGENT_PREFIX = "fs.s3a.user.agent.prefix"
  val READAHEAD_RANGE = "fs.s3a.readahead.range"
  val INPUT_FADVISE = "fs.s3a.experimental.input.fadvise"
  val SEQUENTIAL_IO = "sequential"
  val NORMAL_IO = "normal"
  val RANDOM_IO = "random"

  val SPARK_PARQUET_COMMITTER_CLASS = "spark.sql.parquet.output.committer.class"

  /**
   * What buffer to use.
   * Default is `FAST_UPLOAD_BUFFER_DISK`
   */
  val FAST_UPLOAD_BUFFER = "fs.s3a.fast.upload.buffer"
  /**
   * Buffer blocks to disk.
   * Capacity is limited to available disk space.
   */
  val FAST_UPLOAD_BUFFER_DISK = "disk"
  /**
   * Use an in-memory array. Fast but will run of heap rapidly.
   */
  val FAST_UPLOAD_BUFFER_ARRAY = "array"
  /**
   * Use a byte buffer. May be more memory efficient than the
   * `FAST_UPLOAD_BUFFER_ARRAY`.
   */
  val FAST_UPLOAD_BYTEBUFFER = "bytebuffer"

  val S3A_COMMITTER_NAME = "fs.s3a.committer.name"

  val OUTPUTCOMMITTER_FACTORY_CLASS = S3ACommitterConstants.OUTPUTCOMMITTER_FACTORY_CLASS

  val OUTPUTCOMMITTER_FACTORY_DEFAULT = S3ACommitterConstants.DEFAULT_COMMITTER_FACTORY


  val S3_CLIENT_FACTORY_IMPL = "fs.s3a.s3.client.factory.impl"
  val DEFAULT_S3_CLIENT_FACTORY = "org.apache.hadoop.fs.s3a.DefaultS3ClientFactory"
  val INCONSISTENT_S3_CLIENT_FACTORY_IMPL =
    "org.apache.hadoop.fs.s3a.InconsistentS3ClientFactory"

  /**
   * Inconsistency (visibility delay) injection settings.
   */
  val FAIL_INJECT_INCONSISTENCY_KEY = "fs.s3a.failinject.inconsistency.key.substring"

  val FAIL_INJECT_INCONSISTENCY_MSEC = "fs.s3a.failinject.inconsistency.msec"

  val FAIL_INJECT_INCONSISTENCY_PROBABILITY = "fs.s3a.failinject.inconsistency.probability"
  
  val INCONSISTENT_PATH = "DELAY_LISTING_ME"
  /**
   * How many seconds affected keys will be delayed from appearing in listing.
   * This should probably be a config value.
   */
  val DEFAULT_DELAY_KEY_MSEC= 2 * 1000

  val DEFAULT_DELAY_KEY_PROBABILITY = 1.0f

  /** Whether or not to allow MetadataStore to be source of truth. */
  val METADATASTORE_AUTHORITATIVE = "fs.s3a.metadatastore.authoritative"
  val DEFAULT_METADATASTORE_AUTHORITATIVE = false

  val S3_METADATA_STORE_IMPL = "fs.s3a.metadatastore.impl"
  /**
   * The region of the DynamoDB service.
   *
   * This config has no default value. If the user does not set this, the
   * S3Guard will operate table in the associated S3 bucket region.
   */
  val S3GUARD_DDB_REGION_KEY = "fs.s3a.s3guard.ddb.region"
  val S3GUARD_DDB_TABLE_CAPACITY_READ_KEY = "fs.s3a.s3guard.ddb.table.capacity.read"
  val S3GUARD_DDB_TABLE_CAPACITY_WRITE_KEY = "fs.s3a.s3guard.ddb.table.capacity.write"
  val S3GUARD_DDB_MAX_RETRIES = "fs.s3a.s3guard.ddb.max.retries"
  val S3GUARD_METASTORE_NULL = "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore"

  /**
   * Use Local memory for the metadata: {@value }.
   * This is not coherent across processes and must be used for testing only.
   */
  val S3GUARD_METASTORE_LOCAL = "org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore"

  /**
   * Use DynamoDB for the metadata: {@value }.
   */
  val S3GUARD_METASTORE_DYNAMO = "org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore"

}
