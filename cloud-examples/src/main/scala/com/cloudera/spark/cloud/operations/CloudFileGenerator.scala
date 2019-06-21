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

package com.cloudera.spark.cloud.operations

import java.io.IOException
import java.net.URI

import com.cloudera.spark.cloud.utils.ConfigSerDeser
import com.cloudera.spark.cloud.ObjectStoreExample
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.PureJavaCrc32

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Generate files containing some numbers in the remote repository.
 */
class CloudFileGenerator extends ObjectStoreExample {

  /**
   * List of the command args for the current example.
   * @return a string
   */
  override protected def usageArgs(): String = {
    "<dest> <months> <files-per-month> <row-count>"
  }

  /**
   * Generate a file containing some numbers in the remote repository.
   * @param sparkConf configuration to use
   * @param args argument array; the first argument must be the destination filename.
   * @return an exit code
   */
  override def action(sparkConf: SparkConf, args: Array[String]): Int = {
    val l = args.length
    if (l < 1) {
      // wrong number of arguments
      return usage()
    }
    val dest = args(0)
    val monthCount = intArg(args, 1, 1)
    val fileCount = intArg(args, 2, 1)
    val rowCount = longArg(args, 3, 1000)
    applyObjectStoreConfigurationOptions(sparkConf, false)
    val sc = new SparkContext(sparkConf)
    try {
      val suffix = ".txt"
      val destURI = new URI(dest)
      val destPath = new Path(destURI)
      val months = 1 to monthCount
      // list of (YYYY, 1), (YYYY, 2), ...
      val monthsByYear = months.flatMap(m =>
        months.map(m => (2016 + m / 12, m % 12))
      )
      val filePerMonthRange = 1 to fileCount
      // build paths like 2016/2016-05/2016-05-0012.txt
      val filepaths = monthsByYear.flatMap { case (year, month) =>
        filePerMonthRange.map(ranger =>
          "%1$04d/%1$04d-%2$02d/%1$04d-%2$02d-%3$04d".format(year, month, ranger) + suffix
        )
      }.map(new Path(destPath, _))
      val fileURIs = filepaths.map(_.toUri)

      val destFS = destPath.getFileSystem(sc.hadoopConfiguration)
      // create the parent directories or fail
      rm(destFS, destPath)
      destFS.mkdirs(destPath.getParent())
      val configSerDeser = new ConfigSerDeser(sc.hadoopConfiguration)
      // RDD to save the text to every path in the files RDD, returning path and
      // the time it took
      val filesRDD = sc.parallelize(fileURIs)
      val putDataRDD = filesRDD.map(uri => {
        val jobDest = new Path(uri)
        val hc = configSerDeser.get()
        val fs = jobDest.getFileSystem(hc)
        var written = 0
        val crc = new PureJavaCrc32
        val executionTime = time {
          val out = fs.create(jobDest, true)
          var row = 0
          while (row < rowCount) {
            row += 1
            val line = "%08x\n".format(row).getBytes
            out.write(line)
            written += line.length
            crc.update(line, 0, line.length)
          }
          out.close()
          logInfo(s"File System = $fs")
        }
        (jobDest.toUri, written, crc.getValue, executionTime)
      }).cache()

      logInfo(s"Initial File System state = $destFS")

      // Trigger the evaluations of the RDDs
      val (executionResults, collectionTime) = durationOf {
        putDataRDD.collect()
      }
      // use the length of the first file as the length of all of them
      val expectedFileLength: Long = executionResults(0)._2

      val execTimeRDD = putDataRDD.map(_._2)
      val aggregatedExecutionTime = execTimeRDD.sum().toLong
      logInfo(s"Time to generate ${filesRDD.count()} entries ${toHuman(collectionTime)}")
      logInfo(s"Aggregate execution time ${toHuman(aggregatedExecutionTime)}")
      logInfo(s"File System = $destFS")


      // list all files under the path using listFiles; verify size
      val (listing, listDuration) = durationOf(destFS.listFiles(destPath, true))
      logInfo(s"time to list paths under $destPath: $listDuration")
      while (listing.hasNext) {
        val entry = listing.next()
        verifyLength(entry.getPath.toString, expectedFileLength, entry.getLen)
      }

      // do a parallel scan of a directory and count the entries
      val lenAccumulator = sc.longAccumulator("totalsize")
      val dataGlobPath = new Path(destPath, "*/*/*" + suffix)
      val fileContentRDD = sc.wholeTextFiles(dataGlobPath.toUri.toString)
      val fileSizeRdd = fileContentRDD.map(record => {
        val actual = record._2.length
        val name = record._1
        verifyLength(name, expectedFileLength, actual)
        lenAccumulator.add(actual)
        actual
      })

      logDuration("Verify the length of all the files")(fileSizeRdd.count())

    } finally {
      logInfo("Stopping Spark Context")
      sc.stop()
    }
    0
  }

  // verify that the length of a listed file is that expected
  def verifyLength(name: String, expectedFileLength: Long, actual: Long): Unit = {
    if (expectedFileLength != actual) {
      throw new IOException(
        s"Expected length of ${name}: $expectedFileLength;" +
          s" actual $actual")
    }
  }

}

 object CloudFileGenerator {

  def main(args: Array[String]) {
    new CloudFileGenerator().run(args)
  }
}
