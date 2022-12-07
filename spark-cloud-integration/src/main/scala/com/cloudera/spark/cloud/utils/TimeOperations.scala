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

import org.apache.spark.internal.Logging

/**
 * Trait to add timing to operations.
 */
trait TimeOperations extends Logging {

  /**
   * Convert a time in nanoseconds into a human-readable form for logging.
   * @param durationNanos duration in nanoseconds
   * @return a string describing the time
   */
  def toNanos(durationNanos: Long): String = {
    "%,d nS".format(durationNanos)
  }
  /**
   * Convert a time in nanoseconds into a human-readable form for logging.
   * @param durationNanos duration in nanoseconds
   * @return a string describing the time
   */
  def toHuman(durationNanos: Long): String = {
    toSeconds(durationNanos)
  }

  /**
   * Convert a time in nanoseconds into a human-readable form for logging.
   * @param durationNanos duration in nanoseconds
   * @return a string describing the time
   */
  def toSeconds(durationNanos: Long): String = {
    val millis = durationNanos / 1e6
    "%,fs".format((millis / 1000))
  }

  /**
   * Measure the duration of an operation, log it with the text.
   * @param operation operation description
   * @param testFun function to execute
   * @return the result
   */
  def logDuration[T](operation: String)(testFun: => T): T = {
    val (r, d) = logDuration2(operation)(testFun)
    r
  }

  /**
   * Measure the duration of an operation, log it with the text.
   *
   * @param operation operation description
   * @param testFun function to execute
   * @return the result and the operation duration in nanos
   *
   */
  def logDuration2[T](operation: String)(testFun: => T): (T, Long) = {
    logInfo(s"Starting $operation")
    val (r, d) = durationOf(testFun)
    logInfo(s"Duration of $operation = ${toHuman(d)}")
    (r, d)
  }

  /**
   * Measure the duration of an operation, return the result and the duration
   * @param testFun function to execute
   * @return the result and the operation duration in nanos
   */
  def durationOf[T](testFun: => T): (T, Long) = {
    val start = nanos()
    try {
      var r = testFun
      val end = nanos()
      val d = end - start
      (r, d)
    } catch {
      case ex: Exception =>
        val end = nanos()
        val d = end - start
        logWarning(s"After ${toHuman(d)}: $ex", ex)
        throw ex
    }
  }

  /**
   * Measure the duration of an operation.
   * @param testFun function to execute
   * @return the result and the operation start time and duration in nanos
   */
  def duration3[T](testFun: => T): (T, Long, Long) = {
    val start = nanos()
    try {
      var r = testFun
      val end = nanos()
      val d = end - start
      (r, start, d)
    } catch {
      case ex: Exception =>
        val end = nanos()
        val d = end - start
        logError(s"After ${toHuman(d)}: $ex", ex)
        throw ex
    }
  }

  /**
   * Simple measurement of the duration of an operation
   * @param testFun test function
   * @return how long the operation took
   */
  def time(testFun: => Any): Long = {
    val (_, dur) = durationOf(testFun)
    dur
  }

  /**
   * Time in nanoseconds.
   * @return the current time.
   */
  def nanos(): Long = {
    System.nanoTime()
  }

  /**
   * Time in milliseconds
   * @return the current time
   */
  def now(): Long = {
    System.currentTimeMillis()
  }

  /**
   * Spin-wait for a predicate to evaluate to true, sleeping between probes
   * and raising an exception if the condition is not met before the timeout.
   * @param timeout time to wait
   * @param interval sleep interval
   * @param message exception message
   * @param predicate predicate to evaluate
   */
  def await(
      timeout: Long,
      interval: Int = 500,
      message: => String = "timeout")
      (predicate: => Boolean): Unit = {
    val endTime = now() + timeout
    var succeeded = false
    while (!succeeded && now() < endTime) {
      succeeded = predicate
      if (!succeeded) {
        Thread.sleep(interval)
      }
    }
    require(succeeded, message)
  }

}
