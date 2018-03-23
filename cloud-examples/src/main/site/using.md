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

# Using the extra features in these examples

### <a name="streaming"></a>Example: Spark Streaming and Cloud Storage

Spark Streaming can monitor files added to object stores, by
creating a `FileInputDStream` DStream monitoring a path under a bucket.

```scala
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
```


1. The time to scan for new files is proportional to the number of files
under the path —not the number of *new* files, and that it can become a slow operation.
The size of the window needs to be set to handle this.

1. Files only appear in an object store once they are completely written; there
is no need for a worklow of write-then-rename to ensure that files aren't picked up
while they are still being written. Applications can write straight to the monitored directory.



