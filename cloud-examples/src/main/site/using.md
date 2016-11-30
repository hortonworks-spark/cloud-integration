<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
   http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# title


### <a name="dataframes"></a>Example: DataFrames

DataFrames can be created from and saved to object stores through the `read()` and `write()` methods.

```scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

val spark = SparkSession
    .builder
    .appName("DataFrames")
    .config(sparkConf)
    .getOrCreate()
import spark.implicits._
val numRows = 1000

// generate test data
val sourceData = spark.range(0, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))

// define the destination
val dest = "wasb://yourcontainer@youraccount.blob.core.windows.net/dataframes"

// write the data
val orcFile = dest + "/data.orc"
sourceData.write.format("orc").save(orcFile)

// now read it back
val orcData = spark.read.format("orc").load(orcFile)

// finally, write the data as Parquet
orcData.write.format("parquet").save(dest + "/data.parquet")
spark.stop()
```

### <a name="streaming"></a>Example: Spark Streaming and Cloud Storage

Spark Streaming can monitor files added to object stores, by
creating a `FileInputDStream` DStream monitoring a path under a bucket.

{% highlight scala %}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

val sparkConf = new SparkConf()
val ssc = new StreamingContext(sparkConf, Milliseconds(5000))
try {
  val lines = ssc.textFileStream("s3a://bucket/incoming")
  val matches = lines.filter(_.endsWith("3"))
  matches.print()
  ssc.start()
  ssc.awaitTermination()
} finally {
  ssc.stop(true)
}
{% endhighlight %}

1. The time to scan for new files is proportional to the number of files
under the path —not the number of *new* files, and that it can become a slow operation.
The size of the window needs to be set to handle this.

1. Files only appear in an object store once they are completely written; there
is no need for a worklow of write-then-rename to ensure that files aren't picked up
while they are still being written. Applications can write straight to the monitored directory.



#### <a name="checkpointing"></a>Checkpointing Streams to object stores


