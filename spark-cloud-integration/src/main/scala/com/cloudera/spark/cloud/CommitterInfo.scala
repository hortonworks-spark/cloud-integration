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

package com.cloudera.spark.cloud

import com.cloudera.spark.cloud.CommitterBinding.factoryForSchema
import com.cloudera.spark.cloud.utils.HConf
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkConf

/**
 * representation of a committer
 * @param name committe name for s3a manifestation
 * @param factory factory classname
 */
case class CommitterInfo(name: String, factory: String)
  extends HConf {

  def bind(sparkConf: SparkConf): Unit = {
    bindToSchema(sparkConf, "s3a")
  }

  def bind(conf: Configuration): Unit = {
    bindToSchema(conf, "s3a")
  }

  def bindToSchema(sparkConf: SparkConf, fsSchema: String): Unit = {
    hconf(sparkConf, factoryForSchema(fsSchema), factory)
    hconf(sparkConf, CommitterBinding.S3A_COMMITTER_NAME,
      name)
  }

  def bindToSchema(conf: Configuration, fsSchema: String): Unit = {
    conf.set(factoryForSchema(fsSchema), factory)
    conf.set(CommitterBinding.S3A_COMMITTER_NAME, name)
  }

  override def toString: String = s"Committer binding $factory($name)"
}
