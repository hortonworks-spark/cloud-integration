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

package org.apache.hadoop.fs

import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration

/**
 * Help with testing by accessing package-private methods in FileSystem which
 * are designed for aiding testability. They are normally accessed via
 * `FileSystemTestHelper`, but as that is in hadoop-common-test JAR, a simple
 * object here avoids maven import conflict problems.
 */
object FSHelper {

  @throws[IOException]
  def addFileSystemForTesting(uri: URI, conf: Configuration, fs: FileSystem) {
    FileSystem.addFileSystemForTesting(uri, conf, fs)
  }
}
