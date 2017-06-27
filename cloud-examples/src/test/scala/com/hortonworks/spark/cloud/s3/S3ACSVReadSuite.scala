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


import scala.collection.mutable

import com.hortonworks.spark.cloud.CloudSuite
import com.hortonworks.spark.cloud.common.ReadSample
import org.apache.hadoop.fs.{FSDataInputStream, Path, RemoteIterator}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat

import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.HadoopRDD

/**
 * A suite of tests reading in the S3A CSV file.
 */
class S3ACSVReadSuite extends CloudSuite with S3ATestSetup {

  /**
   * Minimum number of lines, from `gunzip` + `wc -l`.
   * This grows over time.
   */
  val ExpectedSceneListLines = 447919

  override def enabled: Boolean = super.enabled && hasCSVTestFile

  init()

  def init(): Unit = {
    if (enabled) {
      setupFilesystemConfiguration(getConf)
    }
  }

  ctest("CSVgz", "Read compressed CSV files through the spark context") {
    val source = CSV_TESTFILE.get
    sc = new SparkContext("local", "CSVgz", newSparkConf(source))
    val fs = getFilesystem(source)
    val sceneInfo = fs.getFileStatus(source)
    logInfo(s"Compressed size = ${sceneInfo.getLen}")
    validateCSV(sc, source)
    logInfo(s"Filesystem statistics $fs")
  }

  /**
   * Validate the CSV by loading it, counting the number of lines and verifying that it
   * is at least as big as that expected. This minimum size test verifies that the source file
   * was read, even when that file is growing from day to day.
   * @param ctx context
   * @param source source object
   * @param lines the minimum number of lines whcih the source must have.
   * @return the actual count of lines read
   */
  def validateCSV(ctx: SparkContext, source: Path, lines: Long = ExpectedSceneListLines): Long = {
    val input = ctx.textFile(source.toString)
    val (count, time) = duration2 {
      input.count()
    }
    logInfo(s" size of $source = $count rows read in $time nS")
    assert(lines <= count,
      s"Number of rows in $source [$count] less than expected value $lines")
    count
  }

  ctest("CSVdiffFS",
    """Use a compressed CSV from the non-default FS.
     | This verifies that the URIs are directing to the correct FS""".stripMargin) {
    // have a default FS of the local filesystem

    sc = new SparkContext("local", "test", newSparkConf(new Path("file://")))
    val source = CSV_TESTFILE.get
    validateCSV(sc, source)
    logInfo(s"Filesystem statistics ${getFilesystem(source)}")
  }


  ctest("FileBlockLocationHandling",
    """Check fileblock locations downgrade from "localhost" to "no location"
      | so that scheduling is across the cluster """
      .stripMargin) {
    // have a default FS of the local filesystem

    sc = new SparkContext("local", "test", newSparkConf(new Path("file://")))
    val source = CSV_TESTFILE.get
    val fs = getFilesystem(source)
    val blockLocations = fs.getFileBlockLocations(source, 0, 1)
    assert(1 === blockLocations.length,
      s"block location array size wrong: ${blockLocations}")
    val hosts = blockLocations(0).getHosts
    assert(1 === hosts.length, s"wrong host size ${hosts}")
    assert("localhost" === hosts(0), "hostname")

    val path = source.toString
    val rdd = sc.hadoopFile[LongWritable, Text, TextInputFormat](path, 1)
    val input = rdd.asInstanceOf[HadoopRDD[_, _]]
    val partitions = input.getPartitions
    val locations = input.getPreferredLocations(partitions.head)
    assert(locations.isEmpty, s"Location list not empty ${locations}")
  }

  ctest("listFiles",
    "List all files under the CSV parent dir") {
    val source = CSV_TESTFILE.get
    val fs = getFilesystem(source)
    val parent = source.getParent
    val files = duration("listFiles") {
      listFiles(fs, parent, true)
    }
    val slice = duration("slice") {
      files.slice(0, 1000)
    }

    val (len, size) = duration("slice.length") {
      slice.map(fs => (1, fs.getLen)).reduce((l, r) => (l._1 + r._1, l._2 + r._2))
    }
    logInfo(s"Length of slice $len, size = $size")
    logInfo(s"Filesystem statistics $fs")
  }

  ctest("ReadBytesReturned",
    """Read in blocks and assess their size and duration.
       | This is to identify buffering quirks. """.stripMargin) {
    val source = CSV_TESTFILE.get
    val fs = getFilesystem(source)
    val blockSize = 8192
    val buffer = new Array[Byte](blockSize)
    val returnSizes: mutable.Map[Int, (Int, Long)] = mutable.Map()
    val stat = fs.getFileStatus(source)
    val blocks = (stat.getLen / blockSize).toInt
    val instream: FSDataInputStream = fs.open(source)
    var readOperations = 0
    var totalReadTime = 0L
    val results = new mutable.MutableList[ReadSample]()
    for (i <- 1 to blocks) {
      var offset = 0
      while (offset < blockSize) {
        readOperations += 1
        val requested = blockSize - offset
        val pos = instream.getPos
        val (bytesRead, started, time) = duration3 {
          instream.read(buffer, offset, requested)
        }
        assert(bytesRead > 0, s"In block $i read from offset $offset returned $bytesRead")
        offset += bytesRead
        totalReadTime += time
        val current = returnSizes.getOrElse(bytesRead, (0, 0L))
        returnSizes(bytesRead) = (1 + current._1, time + current._2)
        val sample = new ReadSample(started, time, blockSize, requested, bytesRead, pos)
        results += sample
      }
    }
    logInfo( s"""$blocks blocks of size $blockSize;
         | total #of read operations $readOperations;
         | total read time=${toHuman(totalReadTime)};
         | ${totalReadTime / (blocks * blockSize)} ns/byte""".stripMargin)


    logInfo("Read sizes")
    returnSizes.toSeq.sortBy(_._1).foreach { v =>
      val returnedBytes = v._1
      val count = v._2._1
      val totalDuration = v._2._2
      logInfo(s"[$returnedBytes] count = $count" +
          s" average duration = ${totalDuration / count}" +
          s" nS/byte = ${totalDuration / (count * returnedBytes)}")
    }

    // spark analysis
    sc = new SparkContext("local", "test", newSparkConf(source))

    val resultsRDD = sc.parallelize(results)
    val blockFrequency = resultsRDD.map(s => (s.blockSize, 1))
        .reduceByKey((v1, v2) => v1 + v2)
        .sortBy(_._2, false)
    logInfo(s"Most frequent sizes:\n")
    blockFrequency.toLocalIterator.foreach { t =>
      logInfo(s"[${t._1}]: ${t._2}\n")
    }
    val resultsVector = resultsRDD.map(_.toVector)
    val stats = Statistics.colStats(resultsVector)
    logInfo(s"Bytes Read ${summary(stats, 4)}")
    logInfo(s"Difference between requested and actual ${summary(stats, 7)}")
    logInfo(s"Per byte read time/nS ${summary(stats, 6)}")
    logInfo(s"Filesystem statistics ${fs}")
  }

  def summary(stats: MultivariateStatisticalSummary, col: Int): String = {
    val b = new StringBuilder(256)
    b.append(s"min=${stats.min(col)}; ")
    b.append(s"max=${stats.max(col)}; ")
    b.append(s"mean=${stats.mean(col)}; ")
    b.toString()
  }

  ctest("getSceneInfo",
    "Get the scene info") {
    val source = CSV_TESTFILE.get
    sc = new SparkContext("local", "CSVgz", newSparkConf(source))
    val fs = getFilesystem(source)
    val sceneInfo = fs.getFileStatus(source)
    logInfo(s"Compressed size = ${sceneInfo.getLen}")
    validateCSV(sc, source)
    logInfo(s"Filesystem statistics ${fs}")
  }

  /**
   * Iterator over remote output.
   *
   * @param source source iterator
   * @tparam T type of response
   */
  class RemoteOutputIterator[T](private val source: RemoteIterator[T]) extends Iterator[T] {
    def hasNext: Boolean = source.hasNext

    def next: T = source.next()
  }

  /**
   * This doesn't do much, except that it is designed to be pasted straight into
   * Zeppelin and work
   */
  ctest("DirOps", "simple directory ops in spark context process") {
    val source = CSV_TESTFILE.get
    sc = new SparkContext("local", "CSVgz", newSparkConf(source))

    import org.apache.hadoop.fs._
    val landsat = "s3a://landsat-pds/scene_list.gz"
    val landsatPath = new Path(landsat)
    val fs = FileSystem.get(landsatPath.toUri, sc.hadoopConfiguration)
    val files = fs.listFiles(landsatPath.getParent, false)
    val listing = new RemoteOutputIterator(files)
    listing.foreach(print(_))

  }

}
