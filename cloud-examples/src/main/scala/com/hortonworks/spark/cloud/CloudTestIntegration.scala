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

import java.io.{File, IOException}
import java.net.URI

import scala.collection.JavaConverters._

import com.hortonworks.spark.cloud.CloudTestKeys._
import com.hortonworks.spark.cloud.s3.S3ACommitterConstants
import com.hortonworks.spark.cloud.utils.ExtraAssertions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FSHelper, FileStatus, FileSystem, LocalFileSystem, LocatedFileStatus, Path}

import org.apache.spark.SparkConf

/**
 * A trait for cloud testing designed to be pluggable in to existing Spark tests,
 * so allowing them to be used for regression testing against object stores.
 *
 * Primarily this deals with FS setup and teardown.
 *
 */
trait CloudTestIntegration extends ExtraAssertions with StoreTestOperations {

  /**
   * The test directory under `/cloud-integration`; derived from the classname
   * This path does not contain any FS binding.
   */
  protected val testDir: Path = {
    new Path("/cloud-integration/" + INCONSISTENT_PATH + "/"
      + this.getClass.getSimpleName)
  }

  /**
   * Accessor to the configuration.
   */
  private val config = CloudSuite.loadConfiguration()

  def getConf: Configuration = {
    config
  }

  /**
   * The filesystem.
   */
  private var _filesystem: Option[FileSystem] = None

  /**
   * Get the filesystem as an option; only valid if enabled and inited
   * @return a filesystem or None
   */
  protected def filesystemOption: Option[FileSystem] = _filesystem

  /**
   * Accessor for the filesystem.
   *
   * @return the filesystem
   */
  protected def filesystem: FileSystem = _filesystem.get

  /**
   * Probe for the filesystem being defined.
   *
   * @return
   */
  protected def isFilesystemDefined: Boolean = _filesystem.isDefined

  /**
   * URI to the filesystem.
   *
   * @return the filesystem URI
   */
  protected def filesystemURI: URI = filesystem.getUri

  /** this system property is always set in a JVM */
  protected val localTmpDir: File = new File(System
    .getProperty("java.io.tmpdir", "/tmp"))
    .getCanonicalFile

  /**
   * Create a test path under the filesystem
   *
   * @param fs filesystem
   * @param testname name of the test
   * @return a fully qualified path under the filesystem
   */
  protected def testPath(fs: FileSystem, testname: String): Path = {
    fs.makeQualified(new Path(testDir, testname))
  }

  /**
   * Create a test path in the test FS
   * @param testname test name (or other unique prefix)
   * @return the path
   */
  protected def path(testname: String): Path = {
    testPath(filesystem, testname)
  }

  /**
   * Delete quietly: any exception message is printed, but no full stack trace.
   * @param p path to delete
   */
  protected def deleteQuietly(p: Path): Unit = {
    try {
      filesystem.delete(p, true)
    } catch {
      case e: IOException =>
        logInfo(s"Deleting $p: ${e.getMessage}")
    }
  }

  /**
   * Update the filesystem; includes a validity check to ensure that the local filesystem
   * is never accidentally picked up.
   *
   * @param fs new filesystem
   */
  protected def setFilesystem(fs: FileSystem): Unit = {
    if (fs.isInstanceOf[LocalFileSystem] || "file" == fs.getScheme) {
      throw new IllegalArgumentException(
        "Test filesystem cannot be local filesystem")
    }
    _filesystem = Some(fs)
  }

  /**
   * Create a filesystem. This adds it as the `filesystem` field.
   * A new instance is created for every test, so that different configurations
   * can be used. If `registerToURI` is set, the instance is put into the FileSystem
   * map of (user, URI) to FileSystem instance, after which it will be picked up by
   * all in-VM code asking for a non-unique FS intance for that URI.
   *
   * @param fsURI filesystem URI
   * @param registerToURI should the `Filesystem.get` binding for this URI be registered
   * to this instance?
   * @return the newly create FS.
   */
  protected def createFilesystem(
      fsURI: URI,
      registerToURI: Boolean = true): FileSystem = {
    val fs = FileSystem.newInstance(fsURI, getConf)
    setFilesystem(fs)
    if (registerToURI) {
      FSHelper.addFileSystemForTesting(fsURI, getConf, fs)
    }
    fs
  }

  /**
   * Explicitly set the FS to be the local FS.
   */
  protected def setLocalFS(): Unit = {
    _filesystem = Some(getLocalFS)
  }

  /**
   * Get the local filesystem
   *
   * @return the local FS.
   */
  protected def getLocalFS: LocalFileSystem = {
    FileSystem.getLocal(getConf)
  }

  /**
   * Clean up the filesystem if it is defined.
   */
  protected def cleanFilesystem(): Unit = {
    val target = s"${filesystem.getUri}$testDir"
    if (filesystem.isInstanceOf[LocalFileSystem]) {
      logDebug(s"not touching local FS $filesystem")
    } else {
      logInfo(s"Cleaning $target")
      if (filesystem.exists(testDir) && !rm(filesystem, testDir)) {
        logWarning(s"Deleting $target returned false")
      }
    }
  }

  protected def cleanFSInTeardownEnabled = true

  /**
   * Teardown-time cleanup; exceptions are logged and not forwarded.
   */
  protected def cleanFilesystemInTeardown(): Unit = {
    try {
      if (cleanFSInTeardownEnabled) {
        cleanFilesystem()
      }
    } catch {
      case e: Throwable =>
        logInfo(s"During cleanup of filesystem: $e")
        logDebug(s"During cleanup of filesystem", e)
    }
  }

  /**
   * Get a required option; throw an exception if the key is missing or an empty string.
   *
   * @param key the key to look up
   * @return the trimmed string value.
   */
  protected def requiredOption(key: String): String = {
    val c = getConf
    require(hasConf(c, key), s"Unset/empty configuration option $key")
    c.getTrimmed(key)
  }

  /**
   * Does a config have an option
   * @param config configuration
   * @param key key to look up
   * @return true if it is found/not empty
   */
  def hasConf(config: Configuration, key: String): Boolean = {
    val v = config.getTrimmed(key)
    v != null && !v.isEmpty
  }

  import com.hortonworks.spark.cloud.ObjectStoreConfigurations._

  /**
   * Override point for suites: a method which is called
   * in all the `newSparkConf()` methods.
   * This can be used to alter values for the configuration.
   * It is called before the configuration read in from the command line
   * is applied, so that tests can override the values applied in-code.
   *
   * @param sparkConf spark configuration to alter
   */
  protected def addSuiteConfigurationOptions(sparkConf: SparkConf): Unit = {
    sparkConf.setAll(GENERAL_SPARK_OPTIONS)
    sparkConf.setAll(FILE_COMMITTER_OPTIONS)
    sparkConf.setAll(ORC_OPTIONS)
    sparkConf.setAll(PARQUET_OPTIONS)
    sparkConf.setAll(HIVE_TEST_SETUP_OPTIONS)
    hconf(sparkConf, BLOCK_SIZE, 1 * 1024 * 1024)
    hconf(sparkConf, MULTIPART_SIZE, MIN_PERMITTED_MULTIPART_SIZE)
    hconf(sparkConf, READAHEAD_RANGE, 128 * 1024)
    hconf(sparkConf, MIN_MULTIPART_THRESHOLD, MIN_PERMITTED_MULTIPART_SIZE)
    hconf(sparkConf, MIN_MULTIPART_THRESHOLD, MIN_PERMITTED_MULTIPART_SIZE)
    hconf(sparkConf, FAST_UPLOAD, true)
    hconf(sparkConf, FAST_UPLOAD_BUFFER, FAST_UPLOAD_BUFFER_ARRAY)
  }

  /**
   * Create a `SparkConf` instance, using the current filesystem as the URI for the default FS.
   * All options loaded from the test configuration XML file will be added as hadoop options.
   *
   * @return the configuration
   */
  def newSparkConf(): SparkConf = {
    requireFileSystem()
    newSparkConf(filesystemURI)
  }

  /**
   * Require a valid filesystem
   */
  protected def requireFileSystem(): Unit = {
    require(isFilesystemDefined, "Not bonded to a test filesystem")
  }

  /**
   * Create a `SparkConf` instance, setting the default filesystem to that of `fsuri`.
   * All options loaded from the test configuration
   * XML file will be added as hadoop options.
   *
   * @param fsuri the URI of the default filesystem
   * @return the configuration
   */
  def newSparkConf(name: String, fsuri: URI): SparkConf = {
    val sparkConf = newSparkConf(fsuri)
    sparkConf.setAppName("DataFrames")
    sparkConf
  }

  /**
   * Create a `SparkConf` instance, setting the default filesystem to that of `fsuri`.
   * All options loaded from the test configuration
   * XML file will be added as hadoop options.
   *
   * @param fsuri the URI of the default filesystem
   * @return the configuration
   */
  def newSparkConf(fsuri: URI): SparkConf = {
    val sc = new SparkConf(false)
    addSuiteConfigurationOptions(sc)
    // patch in the config; this will pull in all of core-site, default
    getConf.asScala.foreach { e =>
      hconf(sc, e.getKey, e.getValue)
    }
    hconf(sc, CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fsuri.toString)
    sc.setMaster("local")
    sc
  }

  /**
   * Creat a new spark configuration with the default FS that of the
   * path parameter supplied.
   *
   * @param path path
   * @return a configuration with the default FS that of the path.
   */
  def newSparkConf(path: Path): SparkConf = {
    newSparkConf(path.getFileSystem(getConf).getUri)
  }

  /**
   * Get the file status of a path.
   *
   * @param path path to query
   * @return the status
   */
  def stat(path: Path): FileStatus = {
    filesystem.getFileStatus(path)
  }

  /**
   * Get the filesystem to a path; uses the current configuration.
   *
   * @param path path to use
   * @return a (cached) filesystem.
   */
  def getFilesystem(path: Path): FileSystem = {
    FileSystem.newInstance(path.toUri, getConf)
  }

  /**
   * Is a staging committer being used? If so, filesystem needs to be
   * set up right.
   *
   * @param config config to probe
   * @return true if this is staging. Not checked: does Hadoop support staging.
   */
  def isUsingStagingCommitter(config: Configuration): Boolean = {
    val committer = config.get(S3ACommitterConstants.S3A_SCHEME_COMMITTER_FACTORY, "")
    committer != null && committer.startsWith(S3ACommitterConstants.STAGING_PACKAGE)
  }

  def assertPathExists(p: Path): Unit = {
    filesystem.getFileStatus(p)
  }

  def assertDirHasFiles(dir: Path): Unit = {
    val entries = ls(dir, true)
    require(entries.exists(!_.isDirectory), s"No files under $dir")
  }

  /**
   * List files in the filesystem.
   *
   * @param path path to list
   * @param recursive flag to request recursive listing
   * @return a sequence of all files (but not directories) underneath the path.
   */
  def ls(
      path: Path,
      recursive: Boolean): Seq[LocatedFileStatus] = {
    listFiles(filesystem, path, recursive)
  }

  /**
   * Log something with newlines above and below, for easier viewing in logs
   * @param info info to print
   */
  def describe(info: => String): Unit = {
    logInfo(s"\n\n$info\n")
  }

  /**
   * Assert that the number of FS entries matches the list; raises an
   * exception if not, including a sorted list of the entries' paths.
   *
   * @param expectedCount expected number of files
   * @param fs fs to list
   * @param dir directory to examine
   */
  def assertFileCount(expectedCount: Int, fs: FileSystem, dir: Path): Unit = {
    val allFiles = listFiles(fs, dir, true).filterNot(
      st => st.getPath.getName.startsWith("_")).toList
    assertFileStatusCount(expectedCount, allFiles)
  }

  /**
   * Assert that the number of FS entries matches the list; raises an
   * exception if not, incluidng a sorted list of the entries' paths
   *
   * @param expectedCount expected number of files
   * @param files file list
   */
  def assertFileStatusCount(
      expectedCount: Int,
      files: List[LocatedFileStatus]): Unit = {
    val len = files.length
    if (len != expectedCount) {
      val listing = files.map(st => st.getPath.toString).sorted.mkString("\n")
      val text = s"Found $len files; expected $expectedCount\n$listing"
      logError(text)
      assert(expectedCount === len, text)
    }
  }
}
