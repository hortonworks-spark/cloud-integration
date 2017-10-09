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

import java.io.IOException
import java.lang.reflect.Method

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, OutputCommitter, TaskAttemptContext}

import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.internal.io.{FileCommitProtocol, HadoopMapReduceCommitProtocol}

/**
 * Spark Commit protocol for Path Output Committers.
 * This committer will work with the `FileOutputCommitter` and subclasses.
 * The original Spark `HadoopMapReduceCommitProtocol` could be reworked
 * use the same interface; doing it here avoids needing any changes to Spark.
 * All implementations *must* be serializable.
 *
 * To support building against other versions of Hadoop. this class uses
 * introspection to check for the existence of the relevant method...it
 * (currently) doesn't actually cast to the relevant operation
 * @param jobId job
 * @param destination destination
 */
class PathOutputCommitProtocol(jobId: String, destination: String)
  extends HadoopMapReduceCommitProtocol(jobId, destination) with Serializable {

  @transient var committer: OutputCommitter = _
  @transient var workPathMethod: Method = _

  logInfo(s"Instantiate committer for job $jobId with path $destination")

  import PathOutputCommitProtocol._

  override protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    logInfo(s"Setting up committer for path $destination")
    val conf = context.getConfiguration
    val factory = buildCommitterFactoryName(context)
    if (factory.isEmpty) {
      throw new IllegalArgumentException("No committer factory defined")
    }
    conf.set(OUTPUTCOMMITTER_FACTORY_CLASS, factory)
    committer = super.setupCommitter(context)
    workPathMethod = resolveWorkPathMethod(committer).getOrElse {
      throw new IllegalArgumentException(s"Committer $committer of type" +
        s" ${committer.getClass} does not support getWorkingDir()")
    }
    logInfo(s"Using committer $committer")
    committer
  }

  /**
   * Build the committer factory name.
   * If "" then the factory is considered undefined.
   * Base implementation: get the value in the configuration itself.
   * @param context task context
   * @return the name of the factory.
   */
  protected def buildCommitterFactoryName(context: TaskAttemptContext): String = {
    context.getConfiguration.getTrimmed(OUTPUTCOMMITTER_FACTORY_CLASS, "")
  }

  private def resolveWorkPathMethod(c: OutputCommitter): Option[Method] = {
    try {
      Some(c.getClass.getDeclaredMethod("getWorkPath"))
    } catch {
      case _: NoSuchMethodException =>
        // no method
        None
    }
  }

  /**
   * Create a temporary file for a task.
   * @param taskContext task context
   * @param dir optional subdirectory
   * @param ext file extension
   * @return a path as a string
   */
  override def newTaskTempFile(
      taskContext: TaskAttemptContext,
      dir: Option[String],
      ext: String): String = {
    val stagingDir = Option(workPathMethod.invoke(committer).asInstanceOf[Path])
      .getOrElse(new Path(destination))
    val parent = dir.map(d => new Path(stagingDir, d))
      .getOrElse(stagingDir)
    val file = new Path(parent, buildFilename(taskContext, ext))
    logDebug(s"Temp file for dir $dir with ext $ext is $file")
    file.toString
  }

  /**
   * Absolute files are still renamed into place for now.
   * @param taskContext task
   * @param absoluteDir destination dir
   * @param ext extension
   * @return an absolute path
   */
  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext,
      absoluteDir: String,
      ext: String): String = {
    val file = super.newTaskTempFileAbsPath(taskContext, absoluteDir, ext)
    logInfo(s"Temp file with absolute path for dir $absoluteDir with ext $ext is $file")
    file
  }

  /**
   * Build a filename which is unique across all task events.
   * It does not have to be consistent across multiple attempts of the same
   * task or job.
   * @param taskContext task context
   * @param ext extension
   * @return a name for a file which must be unique across all task attempts
   */
  protected def buildFilename(
      taskContext: TaskAttemptContext,
      ext: String): String = {
    // The file name looks like part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003-c000.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    f"part-$split%05d-$jobId$ext"
  }

  override def setupJob(jobContext: JobContext): Unit = {
    logInfo("setup job")
    super.setupJob(jobContext)
  }

  override def commitJob(
      jobContext: JobContext,
      taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    logInfo(s"commit job with ${taskCommits.length} task commit message(s)")
    super.commitJob(jobContext, taskCommits)
  }

  override def abortJob(jobContext: JobContext): Unit = {
    try {
      super.abortJob(jobContext)
    } catch {
      case e: IOException =>
        logWarning("Abort job failed", e)
    }
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    super.setupTask(taskContext)
  }

  override def commitTask(
      taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    logInfo("Commit task")
    super.commitTask(taskContext)
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    logInfo("Abort task")
    try {
      super.abortTask(taskContext)
    } catch {
      case e: IOException =>
        logWarning("Abort task failed", e)
    }
  }

  override def onTaskCommit(msg: TaskCommitMessage): Unit = {
    logInfo(s"onTaskCommit($msg)")
  }
}

object PathOutputCommitProtocol {

  val OUTPUTCOMMITTER_FACTORY_CLASS = "mapreduce.pathoutputcommitter.factory.class"
}


