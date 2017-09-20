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

import java.io.FileNotFoundException

import com.hortonworks.spark.cloud.CloudSuite
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat

import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.HadoopRDD

/**
 * Basic IO Tests. The test path is cleaned up afterwards.
 */
//noinspection ScalaDeprecation
abstract class BasicIOTests extends CloudSuite {

  after {
    cleanFilesystemInTeardown()
  }

  ctest("mkdirs", "Simple test of directory operations") {
    val path = testPath(filesystem, "mkdirs")
    filesystem.mkdirs(path)
    val st = stat(path)
    logInfo(s"Created filesystem entry $path: $st")
    filesystem.listFiles(path, true)

    // delete then verify that it is gone
    rm(filesystem, path)
    intercept[FileNotFoundException] {
      val st2 = stat(path)
      logError(s"Got status $st2")
    }
  }

  ctest("FileOutput",
    """Use the classic File Output Committer to commit work.
      | This committer has race and failure conditions, with the commit being O(bytes)
      | and non-atomic.
    """.stripMargin) {
    sc = new SparkContext("local", "test", newSparkConf())
    val conf = sc.hadoopConfiguration
    assert(filesystemURI.toString === conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY))
    val entryCount = testEntryCount
    val numbers = sc.parallelize(1 to entryCount)
    val output = testPath(filesystem, "FileOutput")
    val path = output.toString
    numbers.saveAsTextFile(path)
    val st = eventuallyGetFileStatus(filesystem, output)
    assert(st.isDirectory, s"Not a dir: $st")

    // child entries that aren't just the SUCCESS marker
    val children = filesystem.listStatus(output)
      .filter(_.getPath.getName != "_SUCCESS")
    assert(children.nonEmpty, s"No children under $output")

    children.foreach { child =>
      logInfo(s"$child")
      assert(child.getLen > 0, s"empty output $child")
      assert(child.getBlockSize > 0, s"Zero blocksize in $child")
    }
    val parts = children.flatMap { child =>
      if (child.getLen > 0) Seq(child) else Nil
    }
    assert(1 === parts.length)
    val parts0 = parts(0)
    // now read it in
    val input = sc.textFile(parts0.getPath.toString)
    val results = input.collect()
    assert(entryCount === results.length, s"size of results read in from $parts0")

    val blockLocations = filesystem.getFileBlockLocations(parts0, 0, 1)
    assert(1 === blockLocations.length,
      s"block location array size wrong: ${blockLocations}")
    val hosts = blockLocations(0).getHosts
    assert(1 === hosts.length, s"wrong host size ${hosts}")
    assert("localhost" === hosts(0), "hostname")

    val hadoopRdd = sc.hadoopFile[LongWritable, Text, TextInputFormat](path, 1)
      .asInstanceOf[HadoopRDD[_, _]]
    val partitions = hadoopRdd.getPartitions
    val locations = hadoopRdd.getPreferredLocations(partitions.head)
    assert(locations.isEmpty, s"Location list not empty ${locations}")

    logInfo(s"Filesystem statistics ${filesystem}")
  }

  ctest("NewHadoopAPI", "Use SparkContext.saveAsNewAPIHadoopFile() to save data to a file") {
    sc = new SparkContext("local", "test", newSparkConf())
    val numbers = sc.parallelize(1 to testEntryCount)
    val destFile = testPath(filesystem, "example1")
    saveTextFile(numbers, destFile)
    val basePathStatus = filesystem.getFileStatus(destFile)
    // check blocksize in file status
    val hadoopUtils = new SparkHadoopUtil
    logDuration("listLeafDir") {
      hadoopUtils.listLeafDirStatuses(filesystem, basePathStatus)
    }
    val (leafFileStatus, _) = durationOf {
      hadoopUtils.listLeafStatuses(filesystem, basePathStatus)
    }
    // files are either empty or have a block size
    leafFileStatus.foreach(s => assert(s.getLen == 0 || s.getBlockSize > 0))

    // and run binary files over it to see if SPARK-6527 is real or not.
    sc.binaryFiles(destFile.toUri.toString, 1).map {
      _ => 1
    }.count()
  }

  ctest("Blocksize", "verify default block size is a viable number") {
    val blockSize = filesystem.getDefaultBlockSize();
    assert(blockSize > 512,
      s"Block size o ${filesystem.getUri} too low for partitioning to work: $blockSize")
  }
}
