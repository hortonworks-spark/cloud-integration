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

import com.hortonworks.spark.cloud.{CloudSuite, RemoteOutputIterator}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * A suite of tests working with encryption.
 * Needs multiple encryption keys to work with
 */
class S3AEncryptionSuite extends CloudSuite with S3ATestSetup {



  override def enabled: Boolean =  {
    val conf = getConf
    super.enabled && hasConf(conf, S3A_ENCRYPTION_KEY_1) &&
      hasConf(conf, S3A_ENCRYPTION_KEY_2)
  }

  init()

  def init(): Unit = {
    if (enabled) {
      initFS()
    }
  }

  override def setupFilesystemConfiguration(config: Configuration): Unit = {
    super.setupFilesystemConfiguration(config)
    config.set(SERVER_SIDE_ENCRYPTION_ALGORITHM, SSE_KMS)
    config.set(SERVER_SIDE_ENCRYPTION_KEY, config.getTrimmed(S3A_ENCRYPTION_KEY_1))
  }

  /**
   * Create an FS with key2
   */
  def createKey2FS(): FileSystem = {
    val config = getConf
    config.set(SERVER_SIDE_ENCRYPTION_ALGORITHM, SSE_KMS)
    config.set(SERVER_SIDE_ENCRYPTION_KEY, config.getTrimmed(S3A_ENCRYPTION_KEY_2))
    FileSystem.newInstance(filesystemURI, config)
  }

  /**
   * Create an FS with key2
   */
  def createUnencryptedFS(): FileSystem = {
    val config = getConf
    config.unset(SERVER_SIDE_ENCRYPTION_ALGORITHM)
    FileSystem.newInstance(filesystemURI, config)
  }

  ctest("TwoKeys", " read and write with two keys") {
    val key1 = filesystem.getConf.get(SERVER_SIDE_ENCRYPTION_KEY)
    logInfo(s"Test key 1 = $key1")

    val dir = path("TwoKeys")
    val key1File = new Path(dir, "key1")
    val hello: String = "hello"
    write(filesystem, key1File, hello)

    val fs2 = createKey2FS()
    val key2 = fs2.getConf.get(SERVER_SIDE_ENCRYPTION_KEY)
    logInfo(s"Test key 2 = $key2")
    assert( key1 != key2, "same key is used for both filesystems")

    val status = fs2.getFileStatus(key1File)
    assert( hello.length === status.getLen, s"wrong length in $status")

    val statuses = fs2.listStatus(dir)
    val data = read(fs2, key1File, 128)
    assert (hello.length === data.length)
    assert (hello === data)

    val unencryptedFS = createUnencryptedFS()
    val dataUnencrypted = read(unencryptedFS, key1File, 128)
    assert(hello === dataUnencrypted)

    unencryptedFS.delete(key1File, false)
    fs2.delete(dir, true)

  }


}
