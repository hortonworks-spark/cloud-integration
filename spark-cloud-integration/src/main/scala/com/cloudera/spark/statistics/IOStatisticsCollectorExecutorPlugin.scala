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

package com.cloudera.spark.statistics

import java.util

import org.apache.hadoop.fs.statistics.IOStatisticsContext

import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.{SparkContext, TaskContext, TaskFailedReason}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.util.TaskCompletionListener

class IOStatisticsCollectorExecutorPlugin extends ExecutorPlugin {

  var context: PluginContext = _

  override def init(
    ctx: PluginContext,
    extraConf: util.Map[String, String]): Unit = {

    context = ctx
    // somehow get the active spark context to register
    // the accumulator
    SparkContext.getActive()

  }
  override def shutdown(): Unit = super.shutdown()

  override def onTaskStart(): Unit = {
    val iostatsCtx:IOStatisticsContext = IOStatisticsContext.getCurrentIOStatisticsContext
    iostatsCtx.reset;
    val acc = new IOStatisticsAccumulator


    val taskContext = TaskContext.get()


    taskContext.register(acc)
    taskContext.addTaskCompletionListener(new TaskCompleted(iostatsCtx))

  }

  override def onTaskSucceeded(): Unit = super.onTaskSucceeded()

  override def onTaskFailed(
    failureReason: TaskFailedReason): Unit = super
    .onTaskFailed(failureReason)

  private class TaskCompleted(
    val acc: IOStatisticsAccumulator,
    val iostatsCtx: IOStatisticsContext) extends TaskCompletionListener {

    override def onTaskCompletion(context: TaskContext): Unit = {
      acc.add(iostatsCtx.getIOStatistics)
    }

  }

  private class SparkListenerImpl extends SparkListener {
    override def onJobStart(
      jobStart: SparkListenerJobStart): Unit = super
      .onJobStart(jobStart)
  }
}


object IOStatisticsCollectorExecutorPlugin {
  val ACCUMULATOR_NAME = "io_statistics"
}