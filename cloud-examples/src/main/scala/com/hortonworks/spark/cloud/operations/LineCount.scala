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

package com.hortonworks.spark.cloud.operations

import java.net.URI

import com.hortonworks.spark.cloud.ObjectStoreExample
import com.hortonworks.spark.cloud.s3.SequentialIO
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkContext}

/**
 * A line count operation
 */
class LineCount extends ObjectStoreExample with SequentialIO {

  /**
   * List of the command args for the current example.
   * @return a string
   */
  override protected def usageArgs(): String = {
    "[<source>] [<dest>]"
  }

  /**
   * Count all lines in a file in a remote object store.
   * This is scoped to be accessible for testing.
   *
   * If there is no destination file, the configuration is patched to allow for S3A
   * anonymous access on Hadoop 2.8+.
   *
   * The option to set the credential provider is not supported on Hadoop 2.6/2.7,
   * so the spark/cluster configuration must contain any credentials needed to
   * authenticate with AWS.
   * @param sparkConf configuration to use
   * @param args argument array; if empty then the default CSV path is used.
   * @return an exit code
   */
  override def action(sparkConf: SparkConf, args: Array[String]): Int = {
    if (args.length > 2) {
      return usage()
    }
    val source = arg(args, 0, defaultSource)
    val dest = arg(args, 1)
    if (source.isEmpty) {
      logError(s"No source for operation")
      return usage()
    }
    val srcURI = new URI(source.get)
    val srcPath = new Path(srcURI)
    logInfo(s"Data Source $srcURI")
    applyObjectStoreConfigurationOptions(sparkConf, false)

    // If there is no destination, switch to the anonymous provider.
    maybeEnableAnonymousAccess(sparkConf, dest)
    var sourceFs: FileSystem = null
    var srcFsInfo: Option[String] = None
    var destFsInfo: Option[String] = None
    val sc = new SparkContext(sparkConf)
    try {
      val conf = sc.hadoopConfiguration
      sourceFs = FileSystem.newInstance(srcURI, conf)

      // this will throw an exception if the source file is missing
      val status = sourceFs.getFileStatus(srcPath)
      logInfo(s"Source details: $status")
      val input = sc.textFile(source.get)
      if (dest.isEmpty) {
        // no destination: just do a count
        val count = duration(s" count $srcPath") {
          input.count()
        }
        logInfo(s"line count = $count")
      } else {
        // destination provided
        val destUri = new URI(dest.get)
        val destPath = new Path(destUri)
        val destFS = destPath.getFileSystem(conf)
        duration("setup dest path") {
          rm(destFS, destPath)
          destFS.mkdirs(destPath.getParent())
        }

        duration("save") {
          // generate less output than in, so that writes and commits are not as slow as they
          // would otherwise be, but still have some realistic workload
          val lineLen = input.map(line => Integer.toHexString(line.length))
          saveTextFile(lineLen, destPath)
/*
          saveAsTextFile(lineLen, destPath, sc.hadoopConfiguration,
            classOf[LongWritable],
            classOf[Text])
*/
          destFsInfo = Some(s"\nFile System $destPath=\n$destFS\n")

        }
      }
      srcFsInfo = Some(s"\nSource File System = $sourceFs\n")
    } finally {
      logInfo("Stopping Spark Context")
      sc.stop()
      srcFsInfo.foreach(logInfo(_))
      destFsInfo.foreach(logInfo(_))
    }
    0
  }

  def defaultSource = {
    Some(S3A_CSV_PATH_DEFAULT)
  }

  def maybeEnableAnonymousAccess(
      sparkConf: SparkConf,
      dest: Option[String]): Unit = {
    if (dest.isEmpty) {
      hconf(sparkConf, AWS_CREDENTIALS_PROVIDER, ANONYMOUS_CREDENTIALS)
    }
  }
}
