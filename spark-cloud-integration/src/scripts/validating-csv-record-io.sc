
/*
Creating data

// download/build latest version of cloudstore
https://github.com/steveloughran/cloudstore/

ABFS = abfs://me@stevelukwest.dfs.core.windows.net/
$CLOUDSTORE = cloudstore-1.0.jar

// create 1M of data, either locally or remotely.
bin/hadoop jar $CLOUDSTORE mkcsv -verbose 1000000 ..+

bin/hadoop jar $CLOUDSTORE mkcsv -verbose 100 /Users/stevel/Play/datasets/csv/rows100.csv

bin/hadoop jar $CLOUDSTORE mkcsv -header -quote -verbose 10000000 /Users/stevel/Play/datasets/csv/rows10M.csv

// upload with bin/hadoop fs -copyFromLocal
bin/hadoop fs -copyFromLocal /Users/stevel/Play/datasets/csv/rows10M.csv $ABFS/csv/


*/
/* Launching shell

// needs more than 1 worker thread; the more you add the noisier the loga get
cd ~/Projects/Releases/spark-cdpd-master/
bin/spark-shell --master local[4]

then load this from wherever you saved it

:load /Users/stevel/Projects/sparkwork/cloud-integration/spark-cloud-integration/src/scripts/validating-csv-record-io.sc

/Users/stevel/Projects/sparkwork/cloud-integration/spark-cloud-integration/src/scripts/validating-csv-record-io.sc


bin/spark-shell --master local[4] --driver-memory 2g -c spark.hadoop.fs.azure.readaheadqueue.depth=0


*/

// overall log to info
sc.setLogLevel("INFO")

// explicit set of log4j loggings
import java.nio.charset.StandardCharsets
import java.util.zip.CRC32

import scala.collection.mutable.ListBuffer

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.types._

Logger.getLogger("org.apache.fs.abfs").setLevel(Level.DEBUG)

def level(level: Level, logs: List[String]): Unit =
  for (l <- logs) {
    Logger.getLogger(l).setLevel(level)
  }
level(Level.INFO,
  List("org.apache.hadoop.fs.azurebfs.services.AbfsIoUtils",
    "org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingAnalyzer",
    "org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials"))
level(Level.TRACE,
  List("org.apache.hadoop.fs.azurebfs.services.ReadBufferManager"))
level(Level.INFO, List("org.apache.fs.abfs",
  "org.apache.hadoop.fs.azurebfs.services.AbfsInputStream"))
//level(Level.DEBUG, List("org.apache.fs.abfs"))
level(Level.DEBUG, List("org.apache.fs.abfs.services.AbfsClient"))
level(Level.WARN,
  List("org.apache.spark.storage",
    "org.apache.spark.storage.BlockManager",
    "org.apache.spark.executor.Executor"))


val conf= sc.hadoopConfiguration

// set to false and the problem is fixed on CDH
conf.get("fs.azure.enable.readahead")

// set this to more than the size of the file and the problem does not aurface.

conf.get("mapred.min.split.size")
conf.get("mapreduce.input.fileinputformat.split.minsize")


// print fs iostats when fs is closed at end of session
conf.set("fs.iostatistics.logging.level", "info")

// require abfs.url to be set in conf, or from the env var
val abfs = conf.get("abfs.url", "${env.ABFS}")
// commands

import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.hadoop.fs.statistics.IOStatisticsLogging._
import org.apache.hadoop.fs.statistics.IOStatisticsSupport._

// commands
def path(s: String) = new Path(new java.net.URI(s))
def p(s: String) = new Path(new java.net.URI(s))
def p(p: Path, s: String) = new Path(p, s)

def bind(p: Path): FileSystem = p.getFileSystem(conf)
def ls(path: Path) = for (f <- bind(path).listStatus(path)) yield {f}

/**
* given a path string, get the FS and print its IOStats.
*/
def iostats(s: String): String =
  ioStatisticsToPrettyString(retrieveIOStatistics(bind(path(s))))

// ********************************************************************************************
// probe a FS for the bug using path capabilities checks
// ********************************************************************************************

def isVulnerable(fspath: String): Boolean = {
  val p = path(fspath)
  if (!Set("abfs", "abfss").contains(p.toUri.getScheme)) {
    println("not an azure filesystem")
    return false;
  }
  val targetFS = bind(p);
  if (targetFS.hasPathCapability(p, "fs.azure.capability.prefetch.safe")) {
    println("This release has been patched and is safe.")
    return false;
  }
  if (!targetFS.hasPathCapability(p, "fs.capability.paths.acls")) {
    println("The release predates the bugs addition to the codebase")
    return false;
  }
  println("The release is recent enough to contain the bug, and does not have the fix")
  val fsconf = targetFS.getConf
  if (fsconf.getBoolean("fs.azure.enable.readahead", false)) {
    println("readahead disabled (cloudera releases and hadoop 3.3.5+)")
    return false;
  }
  val depth = "fs.azure.readaheadqueue.depth"
  val queue = fsconf.getInt(depth, -1)
  if (queue == 0) {
    println(s"${depth} set to 0: safe")
    return false;
  }
  queue match {
    case -1 =>
      println(s"${depth} will be the default: unsafe")
      true;
    case 0 =>
      println(s"${depth} set to zero: safe")
      false
    case _ =>
      println(s"${depth} set to ${queue}: unsafe")
      false
  }
}


/**
* create an RDD reading the CSV as text lines; no parsing of the records.
*/
def lineRdd(s: String) = {
  sc.hadoopFile(s, classOf[org.apache.hadoop.mapred.TextInputFormat],
    classOf[org.apache.hadoop.io.LongWritable],
    classOf[org.apache.hadoop.io.Text])
}

// replace with your store's copy

val abfsDatasets = s"${abfs}/datasets"
val abfsCsvDatasets = s"${abfsDatasets}/csv"
val avro = "avro"
val parquet = "parquet"
val orc = "orc"
val json = "json"
val abfsAvroDatasets = s"${abfsDatasets}/avro"

val rows1M = s"${abfsCsvDatasets}/rows1M.csv"
val rows10M = s"${abfsCsvDatasets}/rows10M.csv"

// set to the absolute path
val localDatasets = "file:///Users/stevel/Play/datasets/csv"
// various local row datasets
val localRows100 = s"${localDatasets}/rows100.csv"
val localRows100K = s"${localDatasets}/rows100k.csv"
val localRows1M = s"${localDatasets}/rows1M.csv"
val localRows10M = s"${localDatasets}/rows10M.csv"
val localRows100M = s"${localDatasets}/rows100M.csv"
val localRows = localRows10M

val localAvroDatasets = s"${localDatasets}/avro"
val localAvro10M = s"${localAvroDatasets}/avro10M"
//val localAvro100M = s"${localAvroDatasets}/avro100M"
val abfsAvro1M = s"${abfsAvroDatasets}/avro1M"
val abfsAvro10M = s"${abfsAvroDatasets}/avro10M"
//val abfsAvro100M = s"${abfsAvroDatasets}/avro100M"

val localOrcDatasets = s"${localDatasets}/Orc"
val localOrc = s"${localOrcDatasets}/localrows"

val localParquetDatasets = s"${localDatasets}/parquet"
val localParquet10M = s"${localParquetDatasets}/parquet10M"

val abfsParquetDatasets = s"${abfsDatasets}/parquet"
val abfsParquet10M = s"${abfsParquetDatasets}/parquet10M"


val localOrcDatasets = s"${localDatasets}/orc"
val localOrc10M = s"${localOrcDatasets}/orc10M"
val abfsOrcDatasets = s"${abfsDatasets}/orc"
val abfsOrc10M = s"${abfsOrcDatasets}/orc10M"

val localJsonDatasets = s"${localDatasets}/json"
val localJson10M = s"${localJsonDatasets}/json10M"
val abfsJsonDatasets = s"${abfsDatasets}/json"
val abfsJson10M = s"${abfsJsonDatasets}/json10M"
val abfsJson1M = s"${abfsJsonDatasets}/json1"

val localRowsRDD = lineRdd(localRows100)
val rowsRDD = lineRdd(rows1M)

// expected rows
val M1 = 1000000
val M10 = 10000000
val header = 1;
val expected = M1 + header

// ********************************************************************************************
// only here for IDE to work better; comment out in :load
// ********************************************************************************************
val spark: org.apache.spark.sql.SparkSession = _;val sc = new org.apache.spark.SparkContext()

// ********************************************************************************************
// build a csv dataset
// ********************************************************************************************

/**
 * Dataset class.
 * Latest build is "start","rowId","length","dataCrc","data","rowId2","rowCrc","end"
 */
case class CsvRecord(
    start: String,
    rowId: Long,
    length: Long,
    dataCrc: Long,
    data: String,
    rowId2: Long,
    rowCrc: Long,
    end: String)

/**
 * The StructType of the CSV data.
 * "start","rowId","length","dataCrc","data","rowId2","rowCrc","end"
 */
val csvSchema: StructType = {
  new StructType().
    add("start", StringType). /* always "start" */
    add("rowId", LongType).   /* row id when generated. */
    add("length", LongType).  /* length of 'data' string */
    add("dataCrc", LongType). /* crc2 of the 'data' string */
    add("data", StringType).  /* a char from [a-zA-Z0-9], repeated */
    add("rowId2", LongType).  /* row id when generated. */
    add("rowCrc", LongType).  /* crc32 of all previous columns */
    add("end", StringType)    /* always "end" */
}

// permissive parsing of records; the default
val Permissive = "permissive";
val FailFast = "failfast"
val DropMalformed = "dropmalformed"

val CsvReadOptions: Map[String, String] = Map(
  "header" -> "true",
  "ignoreLeadingWhiteSpace" -> "false",
  "ignoreTrailingWhiteSpace" -> "false",
//  "inferSchema" -> "false",
  "multiLine" -> "false")

def csvDataFrame(path: String, mode: String = Permissive): DataFrame =
  spark.read.options(CsvReadOptions).
    option("inferSchema","false").
    option("mode",mode).
    schema(csvSchema).
    csv(path)

// DF with inference by sampling file. cloud performance killer.
def csvDataFrameSchemaInference(path: String): DataFrame =
  spark.read.options(CsvReadOptions).
    option("inferSchema","true").
    schema(csvSchema).
    csv(path)

/**
 * Load a dataaset
 * @param path path
 * @param format file format
 * @param options extra options
 * @return the dataset
 */
def loadDS(path: String, format: String, options: Map[String, String] = Map()):  Dataset[CsvRecord] =
  spark.read.
    schema(csvSchema).
    format(format).
    options(options).
    load(path).
    as[CsvRecord]

// CRC of a string
def crc(s: String): Long = {
  val crc = new CRC32
  crc.update(s.getBytes(StandardCharsets.UTF_8))
  crc.getValue()
}


def isValid(r: CsvRecord): Boolean = {
  r.rowId == r.rowId2 && r.dataCrc == crc(r.data)
}

def validate(r: CsvRecord,  verbose: Boolean): Unit = {
  if (verbose) {
    println(r)
  }
  val errors = ListBuffer.empty[String]
  val rowId = r.rowId
  if (r.start != "start") {
    errors.append(s"invalid 'start' column")
  }
  if (rowId != r.rowId2) {
    errors.append(s"$rowId mismatch with tail rowid ${r.rowId2}")
  }
  val data = if (r.data != null) r.data else ""
  if (r.length != data.length) {
    errors.append(s"Invalid data length. Expected ${r.length} actual ${data.length}")
  }
  val crcd = crc(data)
  if (r.dataCrc != crcd) {
    errors.append(s"Data checksum mismatch Expected ${r.dataCrc} actual ${crcd}.")
  }
  if (r.end != "end") {
    errors.append(s"Invalid 'end' column")
  }
  if (errors.nonEmpty) {
    // trouble. log and then fail with a detailed message
    val message = new StringBuilder(s"Invalid row $rowId : $r. ")
    println(message)
    errors.foreach(println)
    errors.foreach(e => message.append(e).append("; "))
    throw new IllegalStateException(message.mkString)
  }
}

def validateDS(ds: Dataset[CsvRecord], verbose: Boolean = false) = {
  println(s"validating ${ds}")
  ds.foreach(r => validate(r, verbose))
  s"validation completed ${ds}"
}

def saveAs(ds: Dataset[CsvRecord], dest: String, format: String) = {
  println(s"Saving in format ${format} to ${dest}")
  ds.coalesce(1).
    write.
    mode("overwrite").
    format(format).
    save(dest)
  s"Saved in format ${format} to ${dest}"
}

def toAvro(ds: Dataset[CsvRecord], dest: String): Unit = {
  saveAs(ds, dest, avro)
}

def toParquet(ds: Dataset[CsvRecord], dest: String): Unit = {
  saveAs(ds, dest, "parquet")
}

def toOrc(ds: Dataset[CsvRecord], dest: String): Unit = {
  saveAs(ds, dest, "orc")
}

// Parquet options
val ParquetValidateChecksums = "parquet.page.verify-checksum.enabled"

// local rows io to validate operation and DS/DF Work
val localRowsDFI = csvDataFrameSchemaInference(localRows100)
val localRowsDF = csvDataFrame(localRows1M)
val localRowsDS = localRowsDF.as[CsvRecord]
localRowsDF.show()
val localRows10MDS = csvDataFrame(localRows10M).as[CsvRecord]
val localRows100MDS = csvDataFrame(localRows100M).as[CsvRecord]
val localRows100KDS = csvDataFrame(localRows100K).as[CsvRecord]
val localRows100DS = csvDataFrame(localRows100).as[CsvRecord]


val rowsDF= csvDataFrame(rows1M)
val rowsDS = rowsDF.as[CsvRecord]
val rows10DF= csvDataFrame(rows10M)
val rows10MDS = rows10DF.as[CsvRecord]
val rows10MDSValidating = csvDataFrame(rows10M, FailFast).as[CsvRecord]

val abfsAvro10MDS = loadDS(abfsAvro10M, avro)
val abfsAvro1MDS = loadDS(abfsAvro1M, avro)

val localParquet10MDS = loadDS(localParquet10M, parquet)
val abfsParquet10MDS = loadDS(abfsParquet10M, parquet)
val abfsParquet10MDSValidating = loadDS(abfsParquet10M, parquet, Map(ParquetValidateChecksums -> "true"))


val localOrc10MDS = loadDS(localOrc10M, orc)
val abfsOrc10MDS = loadDS(abfsOrc10M, orc)

val abfsJson1MDS = loadDS(abfsJson1M, json)
val abfsJson10MDS = loadDS(abfsJson10M, json)
val abfsJson10MDSValidating = loadDS(abfsJson10M, json, Map("mode" -> FailFast))



isVulnerable(rows1M)

// validate the ds
"to validate the ds\nvalidateDS(localRowsDS)"

// ********************************************************************************************
// experiment begins here
// ********************************************************************************************

// iostats(rows1M)


"""
  |> now run
  | isVulnerable(rows1M)
  |rowsRDD.count() - M1
  |rowsDS.show
  |validateDS(rowsDS)
  |
  |rows10MDS.count - M10
  |validateDS(rows10MDS)
  |
  |
  |abfsAvro10MDS.count() - M10
  |validateDS(abfsAvro10MDS)
  |""".stripMargin


// prepare locally then upload
// tip: use the manifest committer for the interesting success file which you can print with
/*
bin/mapred org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestPrinter $ABFS/datasets/avro/avro10M/_SUCCESS

 */
/*
localRowsRDD.count() - expected
localRows10MDS.count()
validateDS(localRows10MDS)
rowsDS.count()
validateDS(rowsDS)

rows10MDS.count()
validateDS(rows10MDS)

toAvro(localRows10MDS, abfsAvro10M)
toAvro(localRowsDS, abfsAvro1M)
toParquet(localRows10MDS, abfsParquet10M)
validateDS(localRowsAvroDS)
// avro is too compressed for this to be big enough to show problems; use the 10M
validateDS(abfsAvro1MDS)

// 60s without prefetch 63 with
validateDS(abfsAvro10MDS)

// 54s without prefetch
abfsAvro10MDS.count()

toParquet(localRows10MDS, localParquet10M)
toParquet(localRows10MDS, abfsParquet10M)
localParquet10MDS
abfsParquet10MDS.count()

// 33s prefetch
validateDS(abfsParquet10MDS)

toOrc(localRows10MDS, localOrc10M)
toOrc(localRows10MDS, abfsOrc10M)
localOrc10MDS
abfsOrc10MDS.count()

// no prefetch 25s, w/30-p31s
validateDS(abfsOrc10MDS)


saveAs(localRowsDS, abfsJson1M, json)

*/
// this often comes out as a negative number, showing how some records were missed.
// if does work: retry the experiment.
/*
rowsRDD.count() - M1 - header
rowsDS.show
validateDS(rowsDS)

// 10M values plus the header
rowsRDD.count() - M10 - header
validateDS(rows10MDS)

 */



