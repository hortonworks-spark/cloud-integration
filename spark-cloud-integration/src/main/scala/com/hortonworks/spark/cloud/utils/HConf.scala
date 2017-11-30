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

import org.apache.spark.SparkConf

/**
  * A minimal trait purely to set Hadoop configuration values in a Spark
  * Configuration.
  */
trait HConf {
  /**
    * Set a Hadoop option in a spark configuration.
    *
    * @param sparkConf configuration to update
    * @param key       key
    * @param value     new value
    */
  def hconf(sparkConf: SparkConf, key: String, value: String): SparkConf = {
    sparkConf.set(hkey(key), value)
    sparkConf
  }

  /**
    * Set a Hadoop option in a spark configuration.
    *
    * @param sparkConf configuration to update
    * @param key       key
    * @param value     new value
    */

  def hconf(sparkConf: SparkConf, key: String, value: Boolean): SparkConf = {
    sparkConf.set(hkey(key), value.toString)
    sparkConf
  }

  /**
    * Take a Hadoop key, add the prefix to allow it to be added to
    * a Spark Config and then picked up properly later.
    *
    * @param key key
    * @return the new key
    */
  def hkey(key: String): String = {
    "spark.hadoop." + key
  }

  /**
    * Set a long hadoop option in a spark configuration.
    *
    * @param sparkConf configuration to update
    * @param key       key
    * @param value     new value
    */
  def hconf(sparkConf: SparkConf, key: String, value: Long): SparkConf = {
    sparkConf.set(hkey(key), value.toString)
    sparkConf
  }

  /**
    * Set all supplied options to the spark configuration as hadoop options.
    *
    * @param sparkConf Spark configuration to update
    * @param settings  map of settings.
    */
  def hconf(sparkConf: SparkConf,
    settings: Traversable[(String, Object)]): SparkConf = {
    settings.foreach(e => hconf(sparkConf, e._1, e._2.toString))
    sparkConf
  }

}
