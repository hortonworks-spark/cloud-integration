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

import org.apache.hadoop.util.ExitUtil

import org.apache.spark.SparkConf

/**
 * Trait for example applications working with object stores.
 * Offers: entry point, some operations to add configuration parameters to spark contexts,
 * and some methods to help parse arguments.
 */
trait ObjectStoreExample extends ObjectStoreOperations with Serializable {

  /**
   * Exit code for a usage error: -2
   */
  val EXIT_USAGE: Int = -2

  /**
   * Exit code for a general purpose error: -1
   */
  val EXIT_ERROR: Int = -1

  /**
   * Run the command. This is expected to be invoked by a `main()` call in static companion
   * object or similar entry point.
   * Any exception raised is logged at error and then the exit code set to -1.
   * @param args argument array
   */
  def run(args: Array[String]): Unit = {
    execute(action, args)
  }

  /**
   * Action to execute.
   * @param sparkConf configuration to use
   * @param args argument array
   * @return an exit code
   */
  def action(sparkConf: SparkConf, args: Array[String]): Int = {
    0
  }

  /**
   * Parameter overridden action operation; easy to use in tests.
   * @param sparkConf configuration to use
   * @param args list of arguments -they are converted to strings before use
   * @return an exit code
   */
  def action(sparkConf: SparkConf, args: Seq[Any]): Int = {
    action(sparkConf, args.map(_.toString).toArray)
  }

  /**
   * Execute an operation, using its return value as the System exit code.
   * Exceptions are caught, logged and an exit code of -1 generated.
   *
   * @param operation operation to execute
   * @param args list of arguments from the command line
   */
  protected def execute(operation: (SparkConf, Array[String]) => Int, args: Array[String]): Unit = {
    var exitCode = 0
    try {
      val conf = new SparkConf()
      exitCode = operation(conf, args)
    } catch {
      case e: ExitUtil.ExitException =>
        exitCode = e.getExitCode
        if (exitCode != 0) {
          logError(e.toString)
          logDebug("", e)
        }
      case e: Exception =>
        logError(s"Failed to execute operation: $e", e)
        // in case this is caused by classpath problems, dump it out @ debug level
        logDebug(s"Classpath =\n${System.getProperty("java.class.path")}")
        exitCode = EXIT_ERROR
    }
    logInfo(s"Exit code = $exitCode")
    if (exitCode != 0) {
      exit(exitCode)
    }
  }

  /**
   * Exit the system.
   * This may be overridden for tests: code must not assume that it never returns.
   *
   * @param exitCode exit code to exit with.
   */
  def exit(exitCode: Int): Unit = {
    System.exit(exitCode)
  }

  /**
   * Print a string to stdout.
   * @param s string to print
   */
  def print(s: String): Unit = {
    // scalastyle:off println
    System.out.println(s)
    // scalastyle:on println
  }

  /**
   * Print a usage message.
   * @return an exit code to return on usage problems
   */
  def usage(): Int = {
    print(s"Usage: ${this.getClass.getCanonicalName} ${usageArgs()}")
    EXIT_USAGE
  }

  /**
   * List of the command args for the current example.
   * @return a string (default: "")
   */
  protected def usageArgs(): String = {
    ""
  }

  /**
   * Get a specific argument as an int, returning the default value if there
   * is no such argument.
   * @param args argument list
   * @param index index to look up
   * @param defVal default value
   * @return the argument value, possibly the default
   */
  protected def intArg(args: Array[String], index: Int, defVal: Int): Int = {
    longArg(args, index, defVal).toInt
  }

  /**
   * Get a specific argument as a long, returning the default value if there
   * is no such argument.
   * @param args argument list
   * @param index index to look up
   * @param defVal default value
   * @return the argument value, possibly the default
   */
  protected def longArg(args: Array[String], index: Int, defVal: Long): Long = {
    if (args.length > index) args(index).toLong else defVal
  }

  /**
   * Get a specific argument, returning the default value if there
   * is no such argument.
   * @param args argument list
   * @param index index to look up
   * @param defVal default value
   * @return the argument value, possibly the default
   */
  protected def arg(args: Array[String], index: Int, defVal: String): String = {
    arg(args, index, Some(defVal)).get
  }

  /**
   * Get a specific argument, returning the default value if there
   * is no such argument.
   * @param args argument list
   * @param index index to look up
   * @param defVal default value
   * @return the argument value, possibly the default
   */
  protected def arg(args: Array[String], index: Int, defVal: Option[String]):
    Option[String] = {
    if (args.length > index) Some(args(index)) else defVal
  }

  /**
   * Get a specific argument, if present.
   * @param args argument list
   * @param index index to look up
   * @return the argument value, or None
   */
  protected def arg(args: Array[String], index: Int): Option[String] = {
    arg(args, index, None)
  }

}
