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

import org.apache.hadoop.conf.Configuration
import org.scalatest.Assertions

trait ExtraAssertions extends Assertions {

  /**
   * Expect a specific value; raise an assertion if it is not there
   *
   * @param v value
   * @param msg message
   * @tparam T type
   * @return the actual value
   */
  def expectSome[T](v: Option[T], msg: => String): T = {
    v.getOrElse(throw new AssertionError(msg))
  }

  /**
   * Expect a value to be non-null; return it. It will
   * implicitly be non-null in further use.
   *
   * @param v value to check
   * @param msg message for any assertion
   * @tparam T type of value
   * @return
   */
  def expectNotNull[T](v: T, msg: => String): T = {
    if (v != null) v else throw new AssertionError(msg)
  }

  /**
   * Expect a configuration option to be set
   *
   * @param c config
   * @param key kjey to look for
   * @return the set value
   */
  def expectOptionSet(c: Configuration, key: String): String = {
    expectNotNull(c.get(key), s"Unset property ${key}")
  }

}
