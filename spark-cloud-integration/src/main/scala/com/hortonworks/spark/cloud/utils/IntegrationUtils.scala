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

package com.hortonworks.spark.cloud.utils

import java.net.URL

import org.apache.hadoop.util.ExitUtil

/**
 * A class to instantiate for all the general utils
 */
class IntegrationUtils extends TimeOperations with HConf {
  private val E_NO_CLASS = 11

  def findClass(src: String, classname: String): (String, String, URL, Class[_]) = {
    try {
      val loader = this.getClass.getClassLoader
      val res = classname.replaceAll("\\.", "/") + ".class"
      val url = loader.getResource(res)
      val clazz = loader.loadClass(classname)
      (src, classname, url, clazz)
    } catch {
      case e: Exception =>
        throw new ExitUtil.ExitException(E_NO_CLASS,
          s"Failed to findClass Class $classname from $src").initCause(e)
    }
  }
}
