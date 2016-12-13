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

import com.hortonworks.spark.cloud.CloudSuite
import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Generate
 */
private[cloud] abstract class NumbersRddTests extends CloudSuite {

  after {
    cleanFilesystemInTeardown()
  }

  /**
   * cleanup is currently disabled
   */
  override protected def cleanFSInTeardownEnabled: Boolean = true;


  ctest("SaveRDD",
    """Generate an RDD and save it. No attempt is made to validate the output, so that
      | All post-test-setup FS IO which takes place is related to the committer.
    """.stripMargin) {
    val sparkConf = newSparkConf()
    // speculation enabled as it makes committing more complicated
    conf.setBoolean("spark.speculation", true)
    val dest = testPath(filesystem, pathname)
    filesystem.delete(dest, true)
    val context = new SparkContext("local", "test", sparkConf)
    try {
      val conf = context.hadoopConfiguration
      assert(filesystemURI.toString === conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY))
      val entryCount = testEntryCount
      val numbers = context.makeRDD(1 to entryCount)
      logInfo(s"\nGenerating output under $dest\n")
//      val lineLen = numbers.map(line => Integer.toHexString(line))
      saveRDD(numbers, dest)
    } finally {
      context.stop()
    }
    val fsInfo = filesystem.toString.replace("{", "\n{")
    logInfo(s"Filesystem statistics\n $fsInfo")
    var size = 0L
    var files = 0
    var dirs = 0
    val listing = listFiles(filesystem, dest, true).map{ s =>
      size += s.getLen
      if (s.isFile) {
        files += 1
      } else {
        dirs += 1
      }
      val details = if (s.isFile) s" [${s.getLen}]" else ""
      s"  ${s.getPath}$details"
    }
    val text = s"Contents of $dest:\n" + listing.mkString("\n") + "\n" +
        s"Dirs: $dirs, Files: $files, total bytes = $size"
    logInfo(text)
    assert(size > 0, text)
  }

  protected def pathname = {
    "numbers_rdd_tests"
  }

  /**
   * Save the RDD
   *
   * @param numbers RDD to save
   * @param dest destination path
   */
  protected def saveRDD(numbers: RDD[Int], dest: Path): Unit = {
    numbers.saveAsTextFile(dest.toString)
  }

  /**
   * Save the RDD
   * @param numbers RDD to save
   * @param dest destination path
   */
  protected def saveRDDviaMRv2(numbers: RDD[Int], dest: Path): Unit = {
    val numText = numbers.map(x => (new IntWritable(x), new Text("a" * x)))
    numText.saveAsNewAPIHadoopFile[SequenceFileOutputFormat[IntWritable, Text]](
      dest.toString)
  }

}

