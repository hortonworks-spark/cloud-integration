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

package com.hortonworks.spark.cloud

import java.io.{EOFException, File, IOException}
import java.net.URL

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.reflect.ClassTag

import com.fasterxml.jackson.databind.JsonNode
import com.hortonworks.spark.cloud.commit.CommitterConstants
import com.hortonworks.spark.cloud.commit.CommitterConstants._
import com.hortonworks.spark.cloud.s3.S3ACommitterConstants
import com.hortonworks.spark.cloud.utils.TimeOperations
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, LocatedFileStatus, Path, PathFilter, RemoteIterator, StorageStatistics}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf

/**
 * Extra Hadoop operations for object store integration.
 */
trait ObjectStoreOperations extends Logging /*with CloudTestKeys*/ with
  TimeOperations {


  def saveTextFile[T](rdd: RDD[T], path: Path): Unit = {
    rdd.saveAsTextFile(path.toString)
  }

  /**
   * Save this RDD as a text file, using string representations of elements.
   *
   * There's a bit of convoluted-ness here, as this supports writing to any Hadoop FS,
   * rather than the default one in the configuration ... this is addressed by creating a
   * new configuration. Spark makes a bit too much of the RDD private, so the key
   * and value of the RDD needs to be restated
   *
   * *Important*: this doesn't work.
   * @param rdd RDD to save
   * @param keyClass key of RDD
   * @param valueClass value of RDD
   * @param path path
   * @param conf config
   * @tparam T type of RDD
   */
  def saveAsTextFile[T](rdd: RDD[T],
      path: Path,
      conf: Configuration,
      keyClass: Class[_],
      valueClass: Class[_]): Unit = {
    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
    val textClassTag = implicitly[ClassTag[Text]]
    val r = rdd.mapPartitions { iter =>
      val text = new Text()
      iter.map { x =>
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }
    val pairOps = new PairRDDFunctions(r)
    pairOps.saveAsNewAPIHadoopFile(
      path.toUri.toString,
      keyClass, valueClass,
      classOf[TextOutputFormat[NullWritable, Text]],
      conf)
  }

  /**
   * Take a predicate, generate a path filter from it.
   * @param filterPredicate predicate
   * @return a filter which uses the predicate to decide whether to accept a file or not
   */
  def pathFilter(filterPredicate: Path => Boolean): PathFilter = {
    new PathFilter {
      def accept(path: Path): Boolean = filterPredicate(path)
    }
  }

  /**
   * List files in a filesystem.
   * @param fs filesystem
   * @param path path to list
   * @param recursive flag to request recursive listing
   * @return a sequence of all files (but not directories) underneath the path.
   */
  def listFiles(fs: FileSystem, path: Path, recursive: Boolean): Seq[LocatedFileStatus] = {
    remoteIteratorSequence(fs.listFiles(path, recursive))
  }

  /**
   * Take the output of a remote iterator and covert it to a scala sequence. Network
   * IO may take place during the operation, and changes to a remote FS may result in
   * a sequence which is not consistent with any single state of the FS.
   * @param source source
   * @tparam T type of source
   * @return a sequence
   */
  def remoteIteratorSequence[T](source: RemoteIterator[T]): Seq[T] = {
    new RemoteOutputIterator[T](source).toSeq
  }

  /**
   * Put a string to the destination.
   * @param path path
   * @param conf configuration to use when requesting the filesystem
   * @param body string body
   */
  def put(
      path: Path,
      conf: Configuration,
      body: String): Unit = {
    put(path.getFileSystem(conf), path, body)
  }

  /**
   * Put a string to the destination.
   *
   * @param fs dest FS
   * @param path path to file
   * @param body string body
   */
  def put(
      fs: FileSystem,
      path: Path,
      body: String): Unit = {
    val out = fs.create(path, true)
    try {
      IOUtils.write(body, out)
    } finally {
      IOUtils.closeQuietly(out)
    }
  }

  /**
   * Get a file from a path.
   *
   * @param fs filesystem
   * @param path path to file
   * @return the contents of the path
   */
  def get(fs: FileSystem, path: Path): String = {
    val in = fs.open(path)
    try {
      val s= IOUtils.toString(in)
      in.close()
      s
    } finally {
      IOUtils.closeQuietly(in)
    }
  }

  /**
   * Load a file as JSON; fail fast if the file is 0 bytes long
   *
   * @param fs filesystem
   * @param path path to file
   * @return the contents of the path as a JSON node
   */
  def loadJson(fs: FileSystem, path: Path): JsonNode = {
    val status = fs.getFileStatus(path)
    if (status.getLen == 0) {
      throw new EOFException("Empty File: " + path)
    }
    import com.fasterxml.jackson.databind.ObjectMapper
    new ObjectMapper().readTree(get(fs, path))
  }

  /**
   * Save a dataframe in a specific format.
   *
   * @param df dataframe
   * @param dest destination path
   * @param format format
   * @return the path the DF was saved to
   */
  def saveDF(
      df: DataFrame,
      dest: Path,
      format: String): Path = {
    logDuration(s"write to $dest in format $format") {
      df.write.format(format).save(dest.toString)
    }
    dest
  }

  /**
   * Load a dataframe.
   * @param spark spark session
   * @param source source path
   * @param srcFormat format
   * @return the loaded dataframe
   */
  def loadDF(
      spark: SparkSession,
      source: Path,
      srcFormat: String,
      opts: Map[String, String] = Map()): DataFrame = {
    val reader = spark.read
    reader.options(opts)
    reader.format(srcFormat).load(source.toUri.toString)
  }

  def applyOrcSpeedupOptions(spark: SparkSession): Unit = {
    spark.read.option("mergeSchema", "false")
  }

  /**
   * Take a dotted classname and return the resource
   * @param classname classname to look for
   * @return the resource for the .class
   */
  def classnameToResource(classname: String): String = {
    classname.replace('.','/') + ".class"
  }

  /**
   * Get a resource URL or None, if the resource wasn't found
   * @param resource resource to look for
   * @return the URL, if any
   */
  def resourceURL(resource: String): Option[URL] = {
    Option(this.getClass.getClassLoader.getResource(resource))
  }

  /**
   * General spark options
   */
  val GENERAL_SPARK_OPTIONS = Map(
    "spark.ui.enabled" -> "false",
    "spark.driver.allowMultipleContexts" -> "true"
  )

  val ORC_OPTIONS = Map(
    "spark.hadoop.orc.splits.include.file.footer" -> "true",
    "spark.hadoop.orc.cache.stripe.details.size" -> "1000",
    "spark.hadoop.orc.filterPushdown" -> "true")

  val PARQUET_OPTIONS = Map(
    "spark.sql.parquet.mergeSchema" -> "false",
    "spark.sql.parquet.filterPushdown" -> "true"
  )

  /**
   * The name of the committer to use for Parquet.
   */
  val PARQUET_COMMITTER_CLASS =
    CommitterConstants.BINDING_PATH_OUTPUT_COMMITTER_CLASS
//    CommitterConstants.BINDING_PARQUET_OUTPUT_COMMITTER_CLASS

  /**
   * Options for file output committer: algorithm 2 & skip cleanup.
   */
  val FILE_COMMITTER_OPTIONS = Map(
    "spark.hadoop." + FILEOUTPUTCOMMITTER_ALGORITHM_VERSION -> "2",
    "spark.hadoop." + FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED -> "true")


  /**
   * Options for committer setup.
   * 1. Set the commit algorithm to 3 to force failures if the classic
   * committer was ever somehow picked up.
   * 2. Switch parquet to the parquet committer subclass which will
   * then bind to the factory committer.
   */
  val COMMITTER_OPTIONS = Map(
    "spark.hadoop." + FILEOUTPUTCOMMITTER_ALGORITHM_VERSION -> "3",
    "spark.hadoop." + FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED -> "true",
    SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key ->
      PARQUET_COMMITTER_CLASS
  )


  val HIVE_TEST_SETUP_OPTIONS = Map(
    "spark.sql.test" -> "",
    "spark.sql.shuffle.partitions" -> "5",
    "spark.sql.hive.metastore.barrierPrefixes" -> "org.apache.spark.sql.hive.execution.PairSerDe"
  )

  /**
   * Create the JVM's temp dir, and return its path.
   * @return the temp directory
   */
  def createTmpDir(): File = {
    val tmp = new File(System.getProperty("java.io.tmpdir"))
    tmp.mkdirs()
    tmp
  }

  /**
   * Create a temporary warehouse directory for those tests which neeed on.
   * @return a warehouse directory
   */
  def createWarehouseDir(): File = {
    val warehouseDir = File.createTempFile("warehouse", ".db", createTmpDir())
    warehouseDir.delete()
    warehouseDir
  }

  /**
   * Set a Hadoop option in a spark configuration.
   *
   * @param sparkConf configuration to update
   * @param k key
   * @param v new value
   */
  def hconf(sparkConf: SparkConf, k: String, v: String): Unit = {
    sparkConf.set(s"spark.hadoop.$k", v)
  }

  /**
   * Set a Hadoop option in a spark configuration.
   *
   * @param sparkConf configuration to update
   * @param k key
   * @param v new value
   */

  def hconf(sparkConf: SparkConf, k: String, v: Boolean): Unit = {
    sparkConf.set(s"spark.hadoop.$k", v.toString)
  }

  /**
   * Set a long hadoop option in a spark configuration.
   *
   * @param sparkConf configuration to update
   * @param k key
   * @param v new value
   */
  def hconf(sparkConf: SparkConf, k: String, v: Long): Unit = {
    sparkConf.set(s"spark.hadoop.$k", v.toString)
  }

  /**
   * Set all supplied options to the spark configuration as hadoop options.
   *
   * @param sparkConf Spark configuration to update
   * @param settings map of settings.
   */
  def hconf(sparkConf: SparkConf, settings: Traversable[(String, String)]): Unit = {
    settings.foreach(e => hconf(sparkConf, e._1, e._2))
  }

  /**
   * Get a sorted list of the FS statistics.
   */
  def getStorageStatistics(fs: FileSystem): List[StorageStatistics.LongStatistic] = {
    fs.getStorageStatistics.getLongStatistics.asScala.toList
      .sortWith((left, right) => left.getName > right.getName)
  }



  /**
   * Dump the storage stats; logs nothing if there are none
   * @param stats statistics instance
   */
  def dumpFileSystemStatistics(stats: StorageStatistics) : Unit = {
    for (entry <- stats.getLongStatistics) {
      logInfo(s" ${entry.getName} = ${entry.getValue}")
    }
  }

  /**
   * Copy a file across filesystems, through the local machine.
   * There's no attempt to optimise the operation if the
   * src and dest files are on the same FS.
   * @param src source file
   * @param dest destination
   * @param conf config for the FS binding
   * @param overwrite should the dest be overwritten?
   */
  def copyFile(
      src: Path,
      dest: Path,
      conf: Configuration,
      overwrite: Boolean): Unit = {
    val srcFS = src.getFileSystem(conf)
    val sourceStatus = srcFS.getFileStatus(src)
    require(sourceStatus.isFile, s"Not a file $src")
    val sizeKB = sourceStatus.getLen / 1024
    logInfo(s"Copying $src to $dest (${sizeKB} KB)")
    val (outcome, time) = durationOf {
      FileUtil.copy(srcFS,
        sourceStatus,
        dest.getFileSystem(conf),
        dest,
        false, overwrite, conf)
    }
    val durationS = time / (1e9)
    logInfo(s"Copy Duration = $durationS seconds")
    val bandwidth = time / sizeKB
    logInfo(s"Effective copy bandwidth = $bandwidth KB/s")
  }

  /**
   * Write text to a file
   * @param fs filesystem
   * @param p path
   * @param t text, if "" writes an empty file
   */
  def write(fs: FileSystem, p: Path, t: String): Unit = {
    val out = fs.create(p, true)
    try {
      if (!t.isEmpty) {
        out.write(t.getBytes())
      }
    } finally {
      out.close()
    }
  }

  /**
   * Read from the FS up to the length; there is no retry after the first read
   * @param fs filesystem
   * @param p path
   * @param maxLen max buffer size
   * @return the data read
   */
  def read(fs: FileSystem, p: Path, maxLen: Int = 1024): String = {
    val in = fs.open(p)
    val buffer = new Array[Byte](maxLen)
    val len = in.read(buffer)
    new String(buffer, 0, len)
  }

  /**
   * Recursive delete. Special feature: waits for the inconsistency delay
   * both before and after if the fs property has it set to anything
   *
   * @param fs
   * @param path
   * @return
   */
  protected def rm(
      fs: FileSystem,
      path: Path): Boolean = {
    try {
      val r = fs.delete(path, true)
      r
    } catch {
      case e: IOException =>
        throw new IOException(s"Failed to delete $path on $fs $e", e)
    }
  }

  /**
   * Set the base spark/Hadoop/ORC/parquet options to be used in examples.
   * Also patches spark.master to local, unless already set.
   *
   * @param sparkConf spark configuration to patch
   * @param randomIO is the IO expected to be random access?
   */
  protected def applyObjectStoreConfigurationOptions(
      sparkConf: SparkConf,
      randomIO: Boolean): Unit = {
    // commit with v2 algorithm
    sparkConf.setAll(FILE_COMMITTER_OPTIONS)
    sparkConf.setAll(ORC_OPTIONS)
    sparkConf.setAll(PARQUET_OPTIONS)
    if (!sparkConf.contains("spark.master")) {
      sparkConf.set("spark.master", "local")
    }
  }
}

/**
 * Iterator over remote output.
 * @param source source iterator
 * @tparam T type of response
 */
class RemoteOutputIterator[T](private val source: RemoteIterator[T]) extends Iterator[T] {
  def hasNext: Boolean = source.hasNext

  def next: T = source.next()
}

