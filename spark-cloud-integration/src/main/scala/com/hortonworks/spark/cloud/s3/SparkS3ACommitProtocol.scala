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

import com.hortonworks.spark.cloud.commit.PathOutputCommitProtocol
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

/**
 * S3A Specific committer.
 * This class is essentially superfluous.
 * @param jobId job
 * @param path destination path
 */
class SparkS3ACommitProtocol(jobId: String, path: String)
  extends PathOutputCommitProtocol(jobId, path) with Serializable {

  import S3ACommitterConstants._

  protected def committerName: String = DIRECTORY
  protected def committerFactoryName: String = COMMITTERS_BY_NAME(committerName)._2

  /**
   * Build the committer factory name.
   * If "" then the factory is considered undefined.
   * Base implementation: get the value in the configuration itself.
   *
   * @param context task context
   * @return the name of the factory.
   */
  override protected def buildCommitterFactoryName(context: TaskAttemptContext): String = {
    val base = super.buildCommitterFactoryName(context)
    if (!base.isEmpty) {
      logInfo(s"Using configured committer factory $base")
      base
    } else {
      committerFactoryName
    }
  }
}

object SparkS3ACommitProtocol {
  val NAME = "com.hortonworks.spark.cloud.s3.SparkS3ACommitter"

  val BINDING_OPTIONS = Map(
    SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
      classOf[SparkS3ACommitProtocol].getCanonicalName
  )

  /**
   * Bind a spark configuration to this.
   *
   * @param sparkConf configuration to patch
   */
  def bind(sparkConf: SparkConf): Unit = {
    sparkConf.setAll(BINDING_OPTIONS)
  }
}
