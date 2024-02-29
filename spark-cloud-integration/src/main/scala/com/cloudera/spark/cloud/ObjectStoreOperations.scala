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

package com.cloudera.spark.cloud

import java.io.{Closeable, EOFException, File, FileNotFoundException, IOException}
import java.net.URL
import java.nio.charset.Charset

import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.reflect.ClassTag

import CommitterBinding._
import com.cloudera.spark.cloud.utils.{HConf, TimeOperations}
import com.cloudera.spark.cloud.GeneralCommitterConstants._
import com.fasterxml.jackson.databind.JsonNode
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, LocatedFileStatus, Path, PathFilter, RemoteIterator, StorageStatistics}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql._

/**
 * Trait for operations to aid object store integration.
 */
trait ObjectStoreOperations extends Logging /*with CloudTestKeys*/ with
  TimeOperations with HConf {


  def saveTextFile[T](rdd: RDD[T], path: Path): Unit = {
    rdd.saveAsTextFile(path.toString)
  }

  /**
   * Save this RDD as a text file, using string representations of elements
   * and the via the Hadoop mapreduce API, rather than the older mapred API.
   *
   * There's a bit of convoluted-ness here, as this supports writing to any Hadoop FS,
   * rather than the default one in the configuration ... this is addressed by creating a
   * new configuration. Spark makes a bit too much of the RDD private, so the key
   * and value of the RDD needs to be restated
   *
   * @param rdd RDD to save
   * @param path path
   * @param conf config
   * @tparam T type of RDD
   */
  def saveAsNewTextFile[T](rdd: RDD[T],
      path: Path,
      conf: Configuration): Unit = {
    val textRdd = rdd.mapPartitions { iter =>
      val text = new Text()
      iter.map { x =>
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }
    val pairOps = new PairRDDFunctions(textRdd)(
      implicitly[ClassTag[NullWritable]],
      implicitly[ClassTag[Text]],
      null)
    pairOps.saveAsNewAPIHadoopFile(
      path.toUri.toString,
      classOf[NullWritable],
      classOf[Text],
      classOf[TextOutputFormat[NullWritable, Text]],
      conf)
  }

  /**
   * Take a predicate, generate a path filter from it.
   *
   * @param filterPredicate predicate
   * @return a filter which uses the predicate to decide whether to accept a file or not
   */
  def pathFilter(filterPredicate: Path => Boolean): PathFilter = {
    // ignore IDEA if it suggests simplifying this...it won't compile.
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
      IOUtils.write(body, out, "UTF-8")
    } finally {
      closeQuietly(out)
    }
  }

  /**
   * Get a file from a path in the default charset.
   *
   * @param fs filesystem
   * @param path path to file
   * @return the contents of the path
   */
  def get(fs: FileSystem, path: Path): String = {
    val in = fs.open(path)
    try {
      val s= IOUtils.toString(in, Charset.defaultCharset())
      in.close()
      s
    } finally {
      closeQuietly(in)
    }
  }

  def closeQuietly(c: Closeable) = {
    if (c != null) {
      try {
        c.close();
      } catch {
        case e: Exception =>
          logDebug("When closing",  e)
      }
    }
  }
  /**
   * Get a file from a path in the default charset.
   * This is here to ease spark-shell use
   * @param p path string.
   * @param conf configuration for the FS
   * @return the contents of the path
   */
  def get(p: String, conf: Configuration): String = {
    val path = new Path(p)
    get(path.getFileSystem(conf), path)
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
    reader.format(srcFormat)
      .load(source.toUri.toString)
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
    tempDir("warehouse", ".db")
  }

  /**
   * Create a temporary warehouse directory for those tests which neeed on.
   * @return a warehouse directory
   */
  def tempDir(name: String, suffix: String): File = {
    val dir = File.createTempFile(name, suffix, createTmpDir())
    dir.delete()
    dir
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
   */
  def dumpFileSystemStatistics(stats: StorageStatistics) : Unit = {
    for (entry <- stats.getLongStatistics.asScala) {
      logInfo(s" ${entry.getName} = ${entry.getValue}")
    }
  }
/*

  def dumpFileSystemStatistics(): Unit = {

  }
*/

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
      overwrite: Boolean): Boolean = {
    val srcFS = src.getFileSystem(conf)
    val sourceStatus = srcFS.getFileStatus(src)
    require(sourceStatus.isFile, s"Not a file $src")
    if (!overwrite) {
      val destFS = dest.getFileSystem(conf)
      try {
        val destStatus = destFS.getFileStatus(dest)
        if (destStatus.isFile) {
          logInfo(s"Destinaion $dest exists")
          return false
        }
      } catch {
        case _: FileNotFoundException =>
      }
    }
    val sizeKB = sourceStatus.getLen / 1024
    logInfo(s"Copying $src to $dest (${sizeKB} KB)")
    val (_, time) = durationOf {
      FileUtil.copy(srcFS,
        sourceStatus,
        dest.getFileSystem(conf),
        dest,
        false, overwrite, conf)
    }
    val durationS = time / (1e9)
    logInfo(s"Copy Duration = $durationS seconds")
    val bandwidth =  sizeKB / durationS
    logInfo(s"Effective copy bandwidth = $bandwidth KiB/s")
    true
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
   * Recursive delete.
   *
   * @param fs filesystem
   * @param path path to delete
   * @return the restult of the delete
   */
  protected def rm(
      fs: FileSystem,
      path: Path): Boolean = {
    try {
      fs.delete(path, true)
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
    sparkConf.setAll(ObjectStoreConfigurations.RW_TEST_OPTIONS)
    if (!sparkConf.contains("spark.master")) {
      sparkConf.set("spark.master", "local")
    }
  }

  protected def verifyConfigurationOption(sparkConf: SparkConf,
    key: String, expected: String): Unit = {

    val v = sparkConf.get(key)
    require(v == expected,
      s"value of configuration option $key is '$v'; expected '$expected'")

  }

  protected def verifyConfigurationOptions(sparkConf: SparkConf,
    settings: Traversable[(String, String)]): Unit = {
    settings.foreach(t => verifyConfigurationOption(sparkConf, t._1, t._2))

  }

  val Parquet = "parquet"
  val Csv = "csv"
  val Orc = "orc"

  /**
   * Write a dataset.
   *
   * @param dest      destination path
   * @param source    source DS
   * @param format    format
   * @param parted    should the DS be parted by year & month?
   * @param committer name of committer to use in s3a options
   * @param conflict  conflict policy to set in config
   * @param extraOps  extra operations to pass to the committer/context
   * @tparam T type of returned DS
   * @return success data
   */
  def writeDataset[T](
    @transient destFS: FileSystem,
    @transient dest: Path,
    source: Dataset[T],
    summary: String = "",
    format: String = Orc,
    parted: Boolean = true,
    committer: String = PARTITIONED,
    conflict: String = CONFLICT_MODE_FAIL,
    extraOps: Map[String, String] = Map()): Long = {

    val text =
      s"$summary + committer=$committer format $format partitioning: $parted" +
        s" conflict=$conflict"
    val (_, t) = logDuration2(s"write to $dest: $text") {
      val writer = source.write
      if (parted) {
        writer.partitionBy("year", "month")
      }
      writer.mode(SaveMode.Append)
      extraOps.foreach(t => writer.option(t._1, t._2))
      writer.option(S3A_COMMITTER_NAME, committer)
      writer.option(S3A_CONFLICT_MODE, conflict)
      writer
        .format(format)
        .save(dest.toUri.toString)
    }
    t
  }
}

/**
 * Iterator over remote output.
 * @param source source iterator
 * @tparam T type of response
 */
class RemoteOutputIterator[T](private val source: RemoteIterator[T]) extends Iterator[T] {
  def hasNext: Boolean = source.hasNext

  def next(): T = source.next()
}


/**
 * A referenceable instance
 */
object ObjectStoreOperations extends ObjectStoreOperations {

}

/**
 * An object store configurations to play with.
 */
object ObjectStoreConfigurations  extends HConf {

  /**
   * How many cores?
   */
  val EXECUTOR_CORES = 4

  /**
   * General spark options
   */
  val GENERAL_SPARK_OPTIONS: Map[String, String] = Map(
    "spark.ui.enabled" -> "false",
    "spark.driver.allowMultipleContexts" -> "true",
    "spark.executor.cores" -> EXECUTOR_CORES.toString
  )

  /**
   * Options for ORC.
   */
  val ORC_OPTIONS: Map[String, String] = Map(
    "spark.hadoop.orc.splits.include.file.footer" -> "true",
    "spark.hadoop.orc.cache.stripe.details.size" -> "1000",
    "spark.hadoop.orc.filterPushdown" -> "true")

  /**
   * Options for Parquet.
   */
  val PARQUET_OPTIONS: Map[String, String] = Map(
    "spark.sql.parquet.mergeSchema" -> "false",
    "spark.sql.parquet.filterPushdown" -> "true"
  )

  val ALL_READ_OPTIONS: Map[String, String] =
    GENERAL_SPARK_OPTIONS ++ ORC_OPTIONS ++ PARQUET_OPTIONS

  /**
   * Options for file output committer: algorithm 2 & skip cleanup.
   */
  val FILE_COMMITTER_OPTIONS: Map[String, String] = Map(
    hkey(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION) -> "2",
    
    (FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED) -> "true")


  /**
   * Options for committer setup.
   * 1. Set the commit algorithm to 3 to force failures if the classic
   * committer was ever somehow picked up.
   * 2. Switch parquet to the parquet committer subclass which will
   * then bind to the factory committer.
   * 3.Set spark.sql.sources.commitProtocolClass to  PathOutputCommitProtocol
   */
  val COMMITTER_OPTIONS: Map[String, String] = Map(
    "spark.sql.parquet.output.committer.class" ->
      BINDING_PARQUET_OUTPUT_COMMITTER_CLASS,
    "spark.sql.sources.commitProtocolClass" ->
      GeneralCommitterConstants.PATH_OUTPUT_COMMITTER_NAME,
    hkey(ABFS_SCHEME_COMMITTER_FACTORY) ->
      ABFS_MANIFEST_COMMITTER_FACTORY,
    hkey(GS_SCHEME_COMMITTER_FACTORY) ->
      MANIFEST_COMMITTER_FACTORY,
    hkey("fs.iostatistics.logging.level") -> "info",
    hkey("mapreduce.manifest.committer.validate.output") -> "true",
    // collect worker thread iostats
    hkey("fs.s3a.committer.experimental.collect.iostatistics") -> "true",
    hkey("fs.iostatistics.thread.level.enabled") -> "true"

    //"fs.s3a.committer.summary.report.directory" -> "todo
  )

  val DYNAMIC_PARTITIONING: Map[String, String] = Map(
    DYNAMIC_PARTITION_OVERWRITE -> DYNAMIC)

  /**
   * Extra options for testing with hive.
   */
  val HIVE_TEST_SETUP_OPTIONS: Map[String, String] = Map(
    "spark.ui.enabled" -> "false",
    "spark.sql.test" -> "",
    "spark.sql.codegen.fallback" -> "true",
    "spark.unsafe.exceptionOnMemoryLeak" -> "true",
    "spark.sql.shuffle.partitions" -> "5",
    "spark.sql.hive.metastore.barrierPrefixes" ->
      "org.apache.spark.sql.hive.execution.PairSerDe",
    "spark.sql.hive.metastore.sharedPrefixes" ->
      "com.amazonaws."
  )

  /**
   * Everything needed for tests.
   */
  val RW_TEST_OPTIONS: Map[String, String] =
    ALL_READ_OPTIONS ++ COMMITTER_OPTIONS ++ HIVE_TEST_SETUP_OPTIONS


  /**
   * Set the options defined in [[COMMITTER_OPTIONS]] on the
   * spark context.
   *
   * Warning: this is purely experimental.
   *
   * @param sparkConf spark configuration to bind.
   */
  def bind(sparkConf: SparkConf): Unit = {
    sparkConf.setAll(COMMITTER_OPTIONS)
  }

  /**
   * Aggressive on the readaheads.
   */
  val ABFS_READAHEAD_HADOOP_OPTIONS : Map[String, String] =
    Map(
      "fs.abfs.readahed.enabled" -> "true",
      "fs.azure.readaheadqueue.depth" -> "4"
    )
}
