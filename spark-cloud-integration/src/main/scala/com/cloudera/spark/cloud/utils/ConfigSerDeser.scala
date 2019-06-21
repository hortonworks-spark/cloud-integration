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

package com.cloudera.spark.cloud.utils

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration

/**
 * Class to make Hadoop configurations serializable; uses the
 * `Writeable` operations to do this.
 * Note: this only serializes the explicitly set values, not any set
 * in site/default or other XML resources.
 * @param conf configuration to serialize
 */
class ConfigSerDeser(var conf: Configuration) extends Serializable {

  private val serialVersionUID = 0xABBA0000

  /**
   * Empty constructor: binds to a `new Configuration()`.
   */
  def this() {
    this(new Configuration())
  }

  /**
   * Get the current configuration.
   * @return the configuration.
   */
  def get(): Configuration = conf

  /**
   * Serializable writer.
   * @param out ouput stream
   */
  private def writeObject (out: ObjectOutputStream): Unit = {
    conf.write(out)
  }

  /**
   * Serializable reader.
   * @param in input
   */
  private def readObject (in: ObjectInputStream): Unit = {
    conf = new Configuration()
    conf.readFields(in)
  }

  /**
   * Handle a read without data; this should never be called, but it
   * is here as a safety mechanism.
   */
  private def readObjectNoData(): Unit = {
    conf = new Configuration()
  }
}
