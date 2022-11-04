
/*
Creating data

// download/build latest version of cloudstore
https://github.com/steveloughran/cloudstore/

ABFS = abfs://stevel-testing@stevelukwest.dfs.core.windows.net/
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


/**
* create an RDD reading the CSV as text lines; no parsing of the records.
*/
def lineRdd(s: String) = {
  sc.hadoopFile(s, classOf[org.apache.hadoop.mapred.TextInputFormat],
    classOf[org.apache.hadoop.io.LongWritable],
    classOf[org.apache.hadoop.io.Text])
}

// replace with your store's copy

val abfsCsvDir = "abfs://stevel-testing@stevelukwest.dfs.core.windows.net/csv"
val rows1M = s"${abfsCsvDir}/rows1M.csv"
val rows10M = s"${abfsCsvDir}/rows10M.csv"

// set to the absolute path
val localDatasets = "file:///Users/stevel/Play/datasets/csv"
// various local row datasets
val localRows100 = s"${localDatasets}/rows100.csv"
val localRows1M = s"${localDatasets}/rows1M.csv"
val localRows10M = s"${localDatasets}/rows10M.csv"
val localRows = localRows10M

val localRowsRDD = lineRdd(localRows100)
val rowsRDD = lineRdd(rows1M)

// expected rows
val M1 = 1000000
val M10 = 10000000
val expected = M1

// ********************************************************************************************
// only here for IDE to work better; comment out in :load
// ********************************************************************************************
val spark: org.apache.spark.sql.SparkSession = _;val sc = new org.apache.spark.SparkContext()

// ********************************************************************************************
// build a csv dataset
// ********************************************************************************************

/**
 * Dataset class.
 */
case class CsvRecord(
    rowId: Long,
    dataCrc: Long,
    data: String,
    rowId2: Long,
    rowCrc: Long)

/**
 * The StructType of the CSV data.
 */
val csvSchema: StructType = {
    new StructType().
    add("rowId", LongType).
    add("dataCrc", LongType).
    add("data", StringType).
    add("rowId2", LongType).
    add("rowCrc", LongType)
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

// CRC of a string
def crc(s: String): Long = {
  val crc = new CRC32
  crc.update(s.getBytes(StandardCharsets.UTF_8))
  crc.getValue()
}

def isValid(r: CsvRecord): Boolean = {
  r.rowId == r.rowId2 && r.dataCrc == crc(r.data)
}

def validate(r: CsvRecord): Unit = {
  val rowId = r.rowId
  if (rowId != r.rowId2) {
    throw new RuntimeException(s"Record #$rowId mismatch with tail rowid ${r.rowId2}")
  }
  if (r.dataCrc != crc(r.data)) {
    throw new RuntimeException(s"Record #$rowId data checksum mismatch ${r}")
  }
}

def validateDS(ds: Dataset[CsvRecord]) = {
  ds.foreach(r => validate(r))
  ds
}

// ********************************************************************************************
// probes for read buffer manager.
// ********************************************************************************************

/* only on some builds.
import org.apache.hadoop.fs.azurebfs.services._

val rbm = ReadBufferManager.getBufferManager()

// info about the RBM; only useful if it has the updated toString() method
def rbmInfo(): String = rbm.toString()

// how many reads were discarded?
def inProgressReadsDiscarded() = rbm.getInProgressBlocksDiscarded()

def resetDiscardedCounters() = rbm.resetBlocksDiscardedCounters()
*/

// local rows io to validate operation and DS/DF Work
val localRowsDFI = csvDataFrameSchemaInference(localRows100)
val localRowsDF = csvDataFrame(localRows1M)
val localRowsDS = localRowsDF.as[CsvRecord]
localRowsDF.show()

val rowsDF= csvDataFrame(rows1M)
val rowsDS = rowsDF.as[CsvRecord]
val rows10DF= csvDataFrame(rows10M)
val rows10DS = rows10DF.as[CsvRecord]

// validate the ds
"to validate the ds\nvalidateDS(localRowsDS)"

// ********************************************************************************************
// experiment begins here
// ********************************************************************************************

// iostats(rows1M)


"""
  |> now run
  |rowsRDD.count() - M1
  |rowsDS.show
  |validateDS(rowsDS)
  |
  |rows10DS.count
  |validateDS(+)
  |""".stripMargin


// this will always count to 0
/*
localRowsRDD.count() - expected
*/
// this often comes out as a negative number, showing how some records were missed.
// if does work: retry the experiment.
/*
rowsRDD.count() - M1
rowsDS.show
validateDS(rowsDS)

// 10M valules
validateDS(rows10DS)

 */



