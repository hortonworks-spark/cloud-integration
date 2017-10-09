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

package com.hortonworks.spark.cloud.commit

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.{PathOutputCommitter, PathOutputCommitterFactory}
import org.apache.hadoop.mapreduce.{JobContext, JobStatus, TaskAttemptContext}

/**
 * This is a special committer which creates the factory for the committer and
 * runs off that. Why does it exist? So that you can explicily instantiate
 * a committer by classname and yet still have the actual implementation
 * driven dynamically by the factory options. This simplifies integration.
 * There's no factory for this, as that would lead to a loop.
 */
class BindingPathOutputCommitter(
    outputPath: Path,
    context: TaskAttemptContext) extends PathOutputCommitter(outputPath, context) {

  private val committer = getFactory(outputPath, context)
    .createOutputCommitter(outputPath, context)


  private def getFactory(outputPath: Path,
      context: TaskAttemptContext): PathOutputCommitterFactory = {
      PathOutputCommitterFactory.getCommitterFactory(
        outputPath,
        context.getConfiguration)
  }

  def getCommitter(): PathOutputCommitter = {
    committer
  }

  override def getOutputPath: Path = {
    committer.getOutputPath
  }

  override def getWorkPath: Path = {
    committer.getWorkPath()
  }

  override def setupTask(taskAttemptContext: TaskAttemptContext): Unit = {
    committer.setupTask(taskAttemptContext)
  }

  override def commitTask(taskAttemptContext: TaskAttemptContext): Unit = {
    committer.commitTask(taskAttemptContext)
  }
  override def abortTask(taskAttemptContext: TaskAttemptContext): Unit = {
    committer.abortTask(taskAttemptContext)
  }

  override def setupJob(jobContext: JobContext): Unit = {
    committer.setupJob(jobContext)
  }

  override def needsTaskCommit(taskAttemptContext: TaskAttemptContext): Boolean = {
    committer.needsTaskCommit(taskAttemptContext)
  }

  //noinspection ScalaDeprecation
  override def cleanupJob(jobContext: JobContext): Unit = {
    committer.cleanupJob(jobContext)
  }

  override def isCommitJobRepeatable(jobContext: JobContext): Boolean = {
    committer.isCommitJobRepeatable(jobContext)
  }

  override def commitJob(jobContext: JobContext): Unit = {
    committer.commitJob(jobContext)
  }

  override def recoverTask(taskAttemptContext: TaskAttemptContext): Unit = {
    committer.recoverTask(taskAttemptContext)
  }

  override def abortJob(
      jobContext: JobContext,
      state: JobStatus.State): Unit = {
    committer.abortJob(jobContext, state)
  }

  //noinspection ScalaDeprecation
  override def isRecoverySupported: Boolean = {
    committer.isRecoverySupported()
  }

  override def isRecoverySupported(jobContext: JobContext): Boolean = {
    committer.isRecoverySupported(jobContext)
  }
}
