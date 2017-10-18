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

# Cloud Committers for Apache Spark


This module contains classes which integrate an Apache Spark job with
the new committer plug in mechanism of Apache Hadoop 
—so supporting high performance and deterministic
committing of work to object stores.

Features

* Supports committing Spark output to a Hadoop compatible filesystem via any
Hadoop committer, the classic `FileOutputCommitter` included.
* When Hadoop is configured to use a cloud-infrastructure specific committer,
uses that committer for committing the output of the Spark job.
* Tested with the new S3A committers, "Directory", "Partitioned and "Magic".
* Includes the support classes need to support Parquet output.


## Requirements

1. Apache Hadoop built with the HADOOP-13786 committer. Implicitly this
means that S3Guard is also supported.

2. A version of Spark compatible with the version this module was build against.

3. The relevant JARs needed to interact with the target filesystem/object store.


## Enabling the committer


To use a committer you need to do a number of things

1. Perform any filesystem-level setup/configuration required by the committer.
1. Declare the Hadoop-level options to use the the new committer for the target
filesystem schemas (here: `s3a://`)
1. Declare any configuration options supported by the committer
1. Declare any Spark SQL options needed to support the new committer

### Identify the Committer.

### FileSystem Level configuration

This is beyond the scope of this document. Consult the Hadoop/Hadoop committer
documents.

### Declare the Hadoop bindings

The underlying Hadoop FileOutputFormat needs to be configured to use an S3-specific
factory of committers, one which will then let us choose which S3A committer to
use:

```
spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a=org.apache.hadoop.fs.s3a.commit.DynamicCommitterFactory
```


### Declare the Spark Binding

Two settings need to be changed here to ensure that when Spark creates a committer
for a normal Hadoop `FileOutputFormat` subclass, it uses the Hadoop 3.1 factory
mechanism.

1. The Spark commit protocol class.
1. For Apache Parquet output: the Parquet committer to use in the Spark `ParquetFileFormat`
classes. 

```
spark.sql.sources.commitProtocolClass=com.hortonworks.spark.cloud.commit.PathOutputCommitProtocol
spark.sql.parquet.output.committer.class=org.apache.hadoop.mapreduce.lib.output.BindingPathOutputCommitter
```

You do not need an equivalent of the Parquet committer option for other formats
(ORC, Avro, CSV, etc); as there is no harm in setting it when not used, it is 
best to set it for all jobs, irrespective of output formats.


```
spark.hadoop.fs.s3a.committer.tmp.path=hdfs://nn1:8088/tmp
```

# Choosng a committer

There are now three S3A-specific committers available

**Directory**: stage data into the local filesystem, then copy up the entire
directory tree to the destination.

**Partition**: stage data into the local filesystem, then copy up the data
to the destination, with a conflict policy designed to support writing to 
invidual partitions.

**Magic**: write data directly to "magic" paths in the filesystem, with
the data only becoming visible in the destination directory tree when
the job is completed.


The choice of committer is selected from the value of `fs.s3a.committer.name`

| value of `fs.s3a.committer.name` |  meaning |
|--------|---------|
| `directory` | directory staging committer |
| `partition` | partition staging committer |
| `magic` | the "magic" committer |


(There is also the option `file` to revert back
to the original file committer)


## The Staging committers: Directory and Partitioned

These stage data locally, upload it to the destination when executors
complete individual tasks, and makes the uploaded files visible when the
job is completed. 

They differ in how conflict in directories are resolved. 

Directory committer: conflict resolution is managed that the level of the
entire directory tree: fail, replace or append works on all the data.

Partitioned committer: conflict resolution is only considered in the final
destination paths of output which is expected to be generated with
a data frame configured to generate partitioned output. The contents
of directories in the destination directory tree which do are not updated
in the job are not examined. As a result, this committer can be used to
efficiently managed in-place updates of data within a large directory
tree of partitioned data.

Prerequisites: 

1. A tempory directory must be specified on the local filesystem, with enough
storage capacity to buffer all data being written by active tasks. Once
a task has completed, it's data is uploaded to S3 and the local storage can
be reused.

1. A consistent cluster-wide filesystem for storage of summary information.
This is generally HDFS. 

## Magic

This alters the Hadoop S3A filesytem client to consider some paths "magic"
(specifically, any path with the name "`__magic`"). Files written under a magic
path are converted to uploads against/relative to the parent directory of the
magic path, and only completed when the job is committed.

This committer will upload the data in blocks, so does not need
any/so much storage as the staging committers (this can be)


*Important*: the magic committer must be considered less stable/tested than
the staging committers.

Prerequisites: 

1. the Magic committer requires a consistent object store, which, for
Amazon S3, means that S3Guard must be enabled (in authoritative or non-authoritative modes).

1. The Hadoop S3A client must be configured to recognise "magic" paths.

Here are the options to enable the magic committer support in both the filesystem
and in the spark committer. This excludes the specific details to enable
S3Guard for the target bucket.

```
spark.hadoop.fs.s3a.committer.committer.name=magic
spark.hadoop.fs.s3a.committer.magic.enabled=true
```



## Supporting per-bucket configuration


The S3A per-bucket configuration feature, means that any option set in `fs.s3a.bucket.BUCKETNAME.option=value`
is mapped to the base option `fs.s3a.option=value` when the s3a: URL has `BUCKETNAME` as its
hostname.


As an example, here are the options needed to enable the magic committer for
the bucket "guarded", while still retaining the default committer as "directory".

```
spark.hadoop.fs.s3a.committer.name=directory
spark.hadoop.fs.s3a.bucket.guarded.committer.name=magic
spark.hadoop.fs.s3a.bucket.guarded.committer.magic.enabled=true

```

