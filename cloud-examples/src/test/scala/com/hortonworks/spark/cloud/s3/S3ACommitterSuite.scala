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


class S3ACommitterSuite extends S3ATestSetup {

  import com.hortonworks.spark.cloud.s3.S3AConstants._

  private val s3aCommitterResource = resourceURL(classnameToResource(S3A_COMMITTER_FACTORY))

  private val s3aCommitterExists = s3aCommitterResource.isDefined

  /*
    <property>
    <name>mapreduce.fileoutputcommitter.factory.class</name>
    <value>org.apache.hadoop.fs.s3a.S3AOutputCommitterFactory</value>
  </property>
   */
  def isS3CommitterEnabled: Boolean = {
    val committer = conf.get(MR_COMMITTER_FACTORY, "")
    S3A_COMMITTER_FACTORY == committer
  }

/*
  ctest("check that the committer is set to s3a", "", isS3CommitterEnabled) {
    val factory = org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory.getFileOutputCommitterFactory(conf)
    assert(factory.isInstanceOf[org.apache.hadoop.fs.s3a.S3AOutputCommitterFactory])
  }
*/

  ctest("locate S3A on CP") {
    val resource = classnameToResource("org.apache.hadoop.fs.s3a.S3AFileSystem")
    val s3a = resourceURL(resource).getOrElse {
      fail(s"No resource $resource")
    }
    logInfo(s"S3AFileSystem is at $s3a")
  }

  ctest("look for FS committer on CP") {
    s3aCommitterResource match {
      case Some(url) => logInfo(s"S3A committer is at $url")
      case None => logInfo("No S3A Committer found on classpath")
    }
  }
}
