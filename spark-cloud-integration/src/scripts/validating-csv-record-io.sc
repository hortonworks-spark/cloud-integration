
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
  return true;
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
val localAvro100M = s"${localAvroDatasets}/avro100M"
val abfsAvro1M = s"${abfsAvroDatasets}/avro1M"
val abfsAvro10M = s"${abfsAvroDatasets}/avro10M"
val abfsAvro100M = s"${localAvroDatasets}/avro100M"

val localOrcDatasets = s"${localDatasets}/Orc"
val localOrc = s"${localOrcDatasets}/localrows"

val localParquetDatasets = s"${localDatasets}/Parquet"
val localParquet = s"${localParquetDatasets}/localrows"


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
    add("start", StringType).
    add("rowId", LongType).
    add("length", LongType).
    add("dataCrc", LongType).
    add("data", StringType).
    add("rowId2", LongType).
    add("rowCrc", LongType).
    add("end", StringType)
}

val CsvReadOptions: Map[String, String] = Map(
  "header" -> "true",
  "mode" -> "FAILFAST",
  "ignoreLeadingWhiteSpace" -> "true",
  "ignoreTrailingWhiteSpace" -> "true",
//  "inferSchema" -> "false",
  "multiLine" -> "false")

def csvDataFrame(path: String): DataFrame =
  spark.read.options(CsvReadOptions).
    option("inferSchema","false").
    schema(csvSchema).
    csv(path)

// DF with inference by sampling file. cloud performance killer.
def csvDataFrameSchemaInference(path: String): DataFrame =
  spark.read.options(CsvReadOptions).
    option("inferSchema","true").
    schema(csvSchema).
    csv(path)


def loadDS(path: String, format: String):  Dataset[CsvRecord] =
  spark.read.
    schema(csvSchema).
    format(format).
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
  ds.foreach(r => validate(r, verbose))
  ds
}

def saveAs(ds: Dataset[CsvRecord], dest: String, format: String): Unit = {
  ds.coalesce(1).
    write.
    mode("overwrite").
    format(format).
    save(dest)
  println(s"Saved in format ${format} to ${dest}")
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
val rows10DS = rows10DF.as[CsvRecord]

val abfsAvro10MDS = loadDS(abfsAvro10M, avro)
val abfsAvro1MDS = loadDS(abfsAvro1M, avro)


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
  |rows10DS.count - M10
  |validateDS(rows10DS)
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
validateDS(rowsDS)

toAvro(localRows10MDS, abfsAvro10M)
toAvro(localRowsDS, abfsAvro1M)
toParquet(localRows10MDS, localAvro)
validateDS(localRowsAvroDS)
// avro is too compressed for this to be big enough to show problems; use the 10M
validateDS(abfsAvro1MDS)
validateDS(abfsAvro10MDS)
abfsAvro10MDS.count()




*/
// this often comes out as a negative number, showing how some records were missed.
// if does work: retry the experiment.
/*
rowsRDD.count() - M1 - header
rowsDS.show
validateDS(rowsDS)

// 10M values plus the header
rowsRDD.count() - M10 - header
validateDS(rows10DS)

 */



