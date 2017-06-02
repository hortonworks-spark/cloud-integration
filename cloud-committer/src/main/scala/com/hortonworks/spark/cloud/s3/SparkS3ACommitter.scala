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

import java.io.IOException

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, OutputCommitter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter
import org.apache.spark.SparkConf
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.internal.io.{FileCommitProtocol, HadoopMapReduceCommitProtocol}
import org.apache.spark.sql.internal.SQLConf

class SparkS3ACommitter(jobId: String, path: String)
  extends HadoopMapReduceCommitProtocol(jobId, path) with Serializable {

  import CommitterConstants._

  @transient var committer: PathOutputCommitter = _

  logInfo(s"Instantiate committer for job $jobId with path $path")

  override protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    logInfo(s"Setting up committer for path $path")
    val conf = context.getConfiguration
    val factory = conf.get(OUTPUTCOMMITTER_FACTORY_CLASS)
    if (factory == null) {
      conf.set(OUTPUTCOMMITTER_FACTORY_CLASS, committerFactoryName)
    } else {
      logInfo(s"Hadoop output committer factory already set to $factory")
    }
    var c = super.setupCommitter(context)
    require(c.isInstanceOf[PathOutputCommitter], s"Committer is wrong type: $c")
    committer = c.asInstanceOf[PathOutputCommitter]
    logInfo(s"Using committer $committer")
    committer
  }

  protected def committerName: String = DIRECTORY
  protected def committerFactoryName: String = COMMITTERS_BY_NAME(committerName)._2

  override def newTaskTempFile(
      taskContext: TaskAttemptContext,
      dir: Option[String],
      ext: String): String = {
    val filename = getFilename(taskContext, ext)

    val stagingDir: String = Option(committer.getWorkPath.toString)
        .getOrElse(path)
    val file = dir.map { d =>
      new Path(new Path(stagingDir, d), filename).toString
    }.getOrElse {
      new Path(stagingDir, filename).toString
    }
    logInfo(s"Temp file for dir ${dir} with ext $ext is $file")
    file
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
    logInfo(s"Temp file for dir $absoluteDir with ext $ext is $file")
    file
  }

  private def getFilename(
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

object SparkS3ACommitter {
  val NAME = "com.hortonworks.spark.cloud.s3.SparkS3ACommitter"

  val BINDING_OPTIONS = Map(
    SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
      classOf[SparkS3ACommitter].getCanonicalName
  )

  /**
   * Bind a spark configuration to this
   *
   * @param sparkConf configuration to patch
   */
  def bind(sparkConf: SparkConf): Unit = {
    sparkConf.setAll(BINDING_OPTIONS)
  }
}
