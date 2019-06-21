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

package com.cloudera.spark.cloud.applications

import java.net.URI

import com.cloudera.spark.cloud.ObjectStoreExample
import com.cloudera.spark.cloud.utils.ConfigSerDeser

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.SparkConf
import org.apache.spark.cloudera.ParallelizedWithLocalityRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Minimal Implementation of DistCP-likewithin Spark.
  *
  * 1. Uses `listFiles(recursive=true)` call for fast tree listing
  * of object store sources.
  * 1. Shuffles source list for better randomness of execution, hence spreading
  * findClass across shards in the store.
  * 1. Schedules each upload individually. This is *very* inefficient
  * for small and empty files.
  * 1. Uses source locality for work scheduling
  *
  *
  * == Possible Improvements ==
  *
  * - Schedule largest objects first so they don't create a long-tail
  * - file:// source performance by switch to copyFromLocalFile() to get
  * benefit of any FS-specific speedup (significant speedup seen in "cloudup").
  * - Handling failures by retrying
  * - Batch up small files so scheduling overhead is less. Locality Needed?
  * - Batch up 0 byte files into larger batches with no locality needed.
  * - Collecting FS statistics from each thread
  *
  * I know distcp does a lot with throttling, but that's  quite hard to do here.
  */
class CloudCp extends ObjectStoreExample {

  override protected def usageArgs(): String = {
    "<source> <dest>"
  }

  /**
    * Action to execute.
    *
    * @param sparkConf configuration to use
    * @param args      argument array
    * @return an exit code
    */
  override def action(
    sparkConf: SparkConf,
    args: Array[String]): Int = {

    def argPath(index: Int): Option[String] = {
      if (args.length > index) {
        Some(args(index))
      } else {
        None
      }
    }

    args.foreach(a => println(a))

    if (args.length != 2) {
      return usage()
    }

    //    val source = CloudTestKeys.S3A_CSV_PATH_DEFAULT
    val srcPath = new Path(args(0))
    val destPath = new Path(args(1))

    logInfo(s"cloudCP $srcPath $destPath")

    sparkConf.set("spark.default.parallelism", "4")
    applyObjectStoreConfigurationOptions(sparkConf, false)
    //    hconf(sparkConf, S3AConstantsAndKeys.FAST_UPLOAD, "true")
    val spark = SparkSession
      .builder
      .appName("CloudCp")
      .config(sparkConf)
      .getOrCreate()

    try {
      val sc = spark.sparkContext
      val sql = sc

      val contextConf = sc.hadoopConfiguration
      val srcFS = srcPath.getFileSystem(contextConf)
      val destConf = new Configuration(contextConf)
      // findClass this FS instance into memory with random
      val destFS = destPath.getFileSystem(destConf)
      logInfo(s"Listing $srcPath for upload to $destPath")

      val srcStr = srcPath.toUri.toString;
      def stripSource(p: Path): String = {
        p.toUri.toString.substring(srcStr.length)
      }


      rm(destFS, destPath)

      // flat list all files and convert to a list of copy operations
      // including block locations
      val copyList = listFiles(srcFS, srcPath, true).map {
        status =>
          val finalDest = new Path(destPath, stripSource(status.getPath))
          new CopySource(status, finalDest)
      }

      val shuffledCopyList = scala.util.Random.shuffle(copyList).toArray
      val copyCount = shuffledCopyList.length
      // parallelize with each line unique. Inefficient for very small files
      // which we may want to batch up more
      val marshalledSrcConfig = new ConfigSerDeser(contextConf)

      // parallelize the copy.
      val copyOperation: RDD[CopyResult] = new ParallelizedWithLocalityRDD(
        sc,
        shuffledCopyList,
        copyCount,
        x => shuffledCopyList(x).locationSeq
      ).map { operation =>
        uploadOneFile(operation, marshalledSrcConfig)
      }

      val (copyResults, duration) = durationOf(copyOperation.collect())
      logInfo(s"Copied $copyResults objects in $duration")
      val totalLen = copyResults.foldLeft(0L)((c, r) => (c + r.len))
      val bandwithBytesNano: Double = if (duration > 0) {
        (totalLen /
          duration)
      } else {
        0
      }
      val bandwidthMB = totalLen / 1e6
      val bandwidthSeconds = bandwidthMB / 1e09
      logInfo(
        s"Total Bytes uploaded $totalLen; bandwidth $bandwidthSeconds MB/s")

    } finally {
      spark.stop()
    }
    0
  }

  /**
    * Copy the file within the worker process.
    *
    * @param operation
    * @param marshalledSrcConfig
    * @return
    */
  def uploadOneFile(operation: CopySource,
    marshalledSrcConfig: ConfigSerDeser): CopyResult = {
    val workerConf = marshalledSrcConfig.get()
    val workerSrc = operation.srcPath
    val workerSrcFS = workerSrc.getFileSystem(workerConf)
    val workerDest = operation.destPath
    val workerDestFS = workerDest.getFileSystem(workerConf)
    // do the copy.
    // This is still recursive on listings.
    val (_, d) = durationOf {
      FileUtil.copy(
        workerSrcFS, workerSrc,
        workerDestFS, workerDest,
        false, true, workerConf)
    }
    CopyResult(operation.source, operation.dest, operation.len, d)
  }

}


/**
  * Marshal a path as a URI
  * @param uri URI to path
  */
class MarshalledPath(uri: URI) extends Serializable {

  private val serialVersionUID = -2766834643529460378L

  require(uri != null)

  /**
    * Create from a path
    * @param path path
    */
  def this(path: Path) = this(path.toUri)

  /**
    * Build a path from the URI.
    * @return a new path instance.
    */
  def path() = new Path(uri)

  override def toString: String = uri.toString
}

/**
  * Reference to a source or dest file; all fields are mutable.
  * @param path
  * @param len
  * @param timestamp
  * @param checksum
  */
case class FileReference(
  var path: MarshalledPath,
  var len: Long,
  var timestamp: Long,
  var checksum: Option[Array[Byte]]) extends Serializable {

  def this(status: FileStatus) = {
    this(new MarshalledPath(status.getPath),
      status.getLen,
      status.getModificationTime,
      None)
  }

  def this(p: Path) = {
    this(new MarshalledPath(p),
      0,
      0,
      None)

  }

  override def toString: String = {
    s"$path, length $len, timestamp $timestamp"
  }

}

/**
  * Hadoop 3.x has serializable Path, but this code strives to work with
  * older versions so marshalls Path as String
  *
  * @param source data
  * @param dest dest
  * @param len
  */
case class CopySource(
  source: FileReference,
  dest: FileReference,
  len: Long,
  locations: Seq[BlockLocation]) extends Serializable {

  private val serialVersionUID = -3560248831387566513L

  def this(status: LocatedFileStatus, dest: Path) = {
    this(
      new FileReference(status),
      new FileReference(dest),
      status.getLen,
      status.getBlockLocations.toList)
  }

  override def toString: String = s"CopySource from ${source.path} to $dest (len $len)"

  override def hashCode(): Int = source.hashCode()

  override def equals(obj: scala.Any): Boolean = source
    .equals(obj.asInstanceOf[CopySource].source)

  def srcPath: Path = source.path.path()

  def destPath: Path = dest.path.path()

  /**
    * Return a list of location sor Nil if there aren't any suitable
    *
    * @return
    */
  def locationSeq: Seq[String] = {
    locations.headOption.map(bl => CloudCp.blockLocationToHost(bl))
      .getOrElse(Nil)
  }
}

case class CopyResult(
  source: FileReference,
  dest: FileReference,
  len: Long,
  duration: Long)  extends Serializable


object CloudCp {

  def main(args: Array[String]) {
    new CloudCp().run(args)
  }

  /**
    * Maps block locations to host. Strips out localhosts
    *
    * @param blockLocation list of block locations
    * @return
    */
  def blockLocationToHost(blockLocation: BlockLocation): Seq[String] = {
    blockLocation.getHosts match {
      case null => Nil
      case hosts => hosts.filterNot(_.equals("localhost"))
    }
  }

}
