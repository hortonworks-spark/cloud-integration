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

package com.hortonworks.spark.cloud.common

import com.hortonworks.spark.cloud.utils.CloudLogging
import org.scalatest.{FunSuite, Matchers}
import org.slf4j.LoggerFactory
import org.slf4j.impl.StaticLoggerBinder

/**
 * Some diagnostics related to logging.
 */
class LoggingCheckSuite extends FunSuite with CloudLogging with Matchers {
  val LogLevels = "com.hortonworks.spark.test.loglevels"
  val Log4JBinding = ""

  test("log4j location") {
    val log4j = this.getClass.getClassLoader.getResource("log4j.properties")
    log4j should not be (null)
    logInfo(s"log4j location = $log4j")
  }

  /**
   * Every time toString is called on this class, it's counter is incremented.
   * this makes it possible to determine when loggers evaluate their arguments
   */
  private class IncrementingToString {
    var c = 0

    override def toString: String = {
      c += 1
      Integer.toString(c)
    }
    
    def value: Int = c
  }

  test("Log4 levels in spark API") {
    val count = new IncrementingToString

    logInfo("Expect 3-4 values, depending on debug level")
    logDebug(s"debug level $count")
    logInfo(s"info level $count")
    logWarning(s"warn level $count")
    logError(s"error level  $count")
    logInfo(s"Total count: ${count.value}")
  }

  test("Log4 levels in logger API") {
    val count = new IncrementingToString
    val l = LoggerFactory.getLogger(LogLevels)
    l.info("Expect 4 values")
    l.debug("debug level {}", count)
    l.info("info level {}", count)
    l.warn("warn level {}", count)
    l.error("error level {}", count)
    l.info("Total count: ", count.value)
    assert (4 === count.value)
  }

  test("Debug level logger") {
    val l = LoggerFactory.getLogger(LogLevels)
    assert(l.isDebugEnabled,
      s"Log level of $LogLevels is not DEBUG $l")
  }

  test("verify SLF4J bindings Log4j") {
    val binder = StaticLoggerBinder.getSingleton();
    logInfo(s"Logger Factory ${binder.getLoggerFactory()}")
    logInfo(s"Logger Factory class ${binder.getLoggerFactoryClassStr()}")
  }

}
