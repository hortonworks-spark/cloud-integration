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
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, PathOutputCommitter, PathOutputCommitterFactory}
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
    jobContext: JobContext) extends PathOutputCommitter(outputPath, jobContext) {

  var factory: Option[PathOutputCommitterFactory] = None
  var committer: Option[PathOutputCommitter] = None
  var taskCommitter: Option[PathOutputCommitter] = None

  def bind(path: Path, taskAttemptContext: TaskAttemptContext): Unit = {
    getCommitter(taskAttemptContext)
  }

  private def getFactory(jobContext: JobContext): PathOutputCommitterFactory = {
    synchronized {
      if (factory.isEmpty) {
        factory = Some(
          PathOutputCommitterFactory.getCommitterFactory(
            getOutputPath(jobContext),
            jobContext.getConfiguration))
      }
      factory.get
    }
  }

  private def getOutputPath(jobContext: JobContext): Path = {
    FileOutputFormat.getOutputPath(jobContext)
  }

  private def getCommitter(context: JobContext): PathOutputCommitter = {
    synchronized {
      if (committer.isEmpty) {
        committer = Some(getFactory(context)
          .createOutputCommitter(getOutputPath(context),
            context))
      }
      committer.get
    }
  }

  private def getCommitter(context: TaskAttemptContext): PathOutputCommitter = {
    synchronized {
      if (committer.isEmpty) {
        committer = Some(getFactory(context)
          .createOutputCommitter(getOutputPath(context),
            context))
      }
      committer.get
    }
  }

  def getCommitter(): PathOutputCommitter = {
    committer.getOrElse {
      throw new IllegalStateException("Committer is not yet initialized")
    }
  }

  override def getOutputPath: Path = {
    getCommitter().getOutputPath
  }

  override def getWorkPath: Path = {
    getCommitter().getWorkPath()
  }

  override def setupTask(taskAttemptContext: TaskAttemptContext): Unit = {
    getCommitter(taskAttemptContext).setupTask(taskAttemptContext)
  }

  override def commitTask(taskAttemptContext: TaskAttemptContext): Unit = {
    getCommitter(taskAttemptContext).commitTask(taskAttemptContext)
  }
  override def abortTask(taskAttemptContext: TaskAttemptContext): Unit = {
    getCommitter(taskAttemptContext).abortTask(taskAttemptContext)
  }

  override def setupJob(jobContext: JobContext): Unit = {
    getCommitter(jobContext).setupJob(jobContext)
  }

  override def needsTaskCommit(taskAttemptContext: TaskAttemptContext): Boolean = {
    getCommitter(taskAttemptContext).needsTaskCommit(taskAttemptContext)
  }

  override def cleanupJob(jobContext: JobContext): Unit = {
    getCommitter(jobContext).cleanupJob(jobContext)
  }

  override def isCommitJobRepeatable(jobContext: JobContext): Boolean = {
    getCommitter(jobContext).isCommitJobRepeatable(jobContext)
  }

  override def commitJob(jobContext: JobContext): Unit = {
    getCommitter(jobContext).commitJob(jobContext)
  }

  override def recoverTask(taskAttemptContext: TaskAttemptContext): Unit = {
    getCommitter(taskAttemptContext).recoverTask(taskAttemptContext)
  }

  override def abortJob(
      jobContext: JobContext,
      state: JobStatus.State): Unit = {
    getCommitter(jobContext).abortJob(jobContext, state)
  }

  override def isRecoverySupported: Boolean = {
    getCommitter().isRecoverySupported()
  }

  override def isRecoverySupported(jobContext: JobContext): Boolean = {
    getCommitter(jobContext).isRecoverySupported(jobContext)
  }
}
