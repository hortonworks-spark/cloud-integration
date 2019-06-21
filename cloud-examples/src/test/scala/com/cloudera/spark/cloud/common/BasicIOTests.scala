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

package com.cloudera.spark.cloud.common

import java.io.FileNotFoundException

import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FileStatus, FileSystem}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat

import org.apache.spark.SparkContext
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
    val sparkConf = newSparkConf()
    sc = new SparkContext("local", "test", sparkConf)
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
      .filter(p => p.isFile && !(p.getPath.getName.startsWith("_")))
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
    saveAsNewTextFile(numbers, destFile, sc.hadoopConfiguration)
    val basePathStatus = filesystem.getFileStatus(destFile)
    // check blocksize in file status
    logDuration("listLeafDir") {
      listLeafDirStatuses(filesystem, basePathStatus)
    }
    val (leafFileStatus, _) = durationOf {
      listLeafStatuses(filesystem, basePathStatus)
    }
    // files are either empty or have a block size
    leafFileStatus.foreach(s => assert(s.getLen == 0 || s.getBlockSize > 0))

    // and run binary files over it to see if SPARK-6527 is real or not.
    sc.binaryFiles(destFile.toUri.toString, 1).map {
      _ => 1
    }.count()
  }

  //noinspection ScalaDeprecation
  ctest("Blocksize", "verify default block size is a viable number") {
    val blockSize = filesystem.getDefaultBlockSize()
    assert(blockSize > 512,
      s"Block size o ${filesystem.getUri} too low for partitioning to work: $blockSize")
  }

  def listLeafDirStatuses(fs: FileSystem,
    baseStatus: FileStatus): Seq[FileStatus] = {
    def recurse(status: FileStatus): Seq[FileStatus] = {
      val (directories, files) = fs.listStatus(status.getPath)
        .partition(_.isDirectory)
      val leaves = if (directories.isEmpty) Seq(status) else Seq
        .empty[FileStatus]
      leaves ++ directories.flatMap(dir => listLeafDirStatuses(fs, dir))
    }

    assert(baseStatus.isDirectory)
    recurse(baseStatus)
  }

  /**
   * Get [[FileStatus]] objects for all leaf children (files) under the given base path. If the
   * given path points to a file, return a single-element collection containing [[FileStatus]] of
   * that file.
   */
  def listLeafStatuses(fs: FileSystem,
    baseStatus: FileStatus): Seq[FileStatus] = {
    def recurse(status: FileStatus): Seq[FileStatus] = {
      val (directories, leaves) = fs.listStatus(status.getPath)
        .partition(_.isDirectory)
      leaves ++ directories.flatMap(f => listLeafStatuses(fs, f))
    }

    if (baseStatus.isDirectory) recurse(baseStatus) else Seq(baseStatus)
  }

}
