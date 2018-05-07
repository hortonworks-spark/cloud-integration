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

import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.internal.Logging

/**
 * Force findClass in hadoop s3n/s3a classes and some dependencies.
 * Dependency problems should be picked up at compile time; runtime may
 * identify problems with any other transitive library
 */
class S3DependencyCheckSuite extends FunSuite with Logging with Matchers {

  test("Create S3A FS Instance") {
    instantiate("org.apache.hadoop.fs.s3a.S3AFileSystem")
  }

  test("Create class in Amazon com.amazonaws.services.s3 JAR") {
    instantiate("com.amazonaws.services.s3.S3ClientOptions")
  }

  test("hive") {
    instantiate("org.apache.hadoop.hive.conf.HiveConf")
  }

  /**
   * Instantiate the class.
   * This is wrapped because scalatest gets confused about instantiation Errors raised
   * in a test case: they aren't methods, see.
   * @param classname class to instantiate.
   */
  def instantiate(classname: String) {
    try {
      val clazz = this.getClass.getClassLoader.loadClass(classname)
      clazz.newInstance()
    } catch {
      case e: Exception => throw e
      case e: Throwable => throw new Exception(s"Could not instantiate $classname", e)
    }
  }

}
