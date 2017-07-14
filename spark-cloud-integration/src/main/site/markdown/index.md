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

# Cloud Committer for Apache Spark


This module contains an ApacheSpark job committer which supports the new committer
plug in mechanism of Apache Hadoop —so supporting high performance and deterministic
committing of work to eventually consistent object stores where the trandition

Features

* Supports committing Spark output to a Hadoop compatible filesystem via any
Hadoop committer, the classic `FileOutputCommitter` included.
* When Hadoop is configured to use a cloud-infrastructure specific committer,
uses that committer for committing the output of the Spark job.
* Tested with the new S3A committers, "Directory", "Partitioned and "Magic".


## Requirements

1. Apache Hadoop built with the HADOOP-13786 committer. Implicitly this
means that S3Guard is also supported.

2. A version of Spark compatible with the version this module was build against.

3. The relevant JARs needed to interact with the target filesystem/object store.


## Enabling the committer


```
spark.sql.sources.commitProtocolClass=com.hortonworks.spark.cloud.s3.SparkS3ACommitter
spark.hadoop.mapreduce.pathoutputcommitter.factory.class=org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitterFactory
```

# Committing to Amazon S3


## Staging committers: Directory and Partitioned

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
spark.sql.sources.commitProtocolClass=com.hortonworks.spark.cloud.s3.SparkS3ACommitter
spark.hadoop.mapreduce.pathoutputcommitter.factory.class=org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory
spark.hadoop.fs.s3a.committer.magic.enabled=true
```

## Supporting per-bucket configuration

There is a special committer factory, "dynamic", whic

```
spark.sql.sources.commitProtocolClass=com.hortonworks.spark.cloud.s3.SparkS3ACommitter
spark.hadoop.mapreduce.pathoutputcommitter.factory.class=org.apache.hadoop.fs.s3a.commit.DynamicCommitterFactory
```

The choice of committer is then chosen from the value of `fs.s3a.committer.name`

| value of `fs.s3a.committer.name` |  meaning |
|--------|---------|
| `file` | the original File committer; (not safe for use with S3 storage) |
| `directory` | directory staging committer |
| `partition` | partition staging committer |
| `magic` | the "magic" committer |

This committer option enables different committers to be used for different
S3 buckets, taking advantage of the S3a per-bucket configuration feature, wherein
any option set in `fs.s3a.bucket.BUCKETNAME.option=value` is mapped to
the base option `fs.s3a.option=value` when the s3a: URL has `BUCKETNAME` as its
hostname.


As an example, here are the options needed to enable the magic committer for
the bucket "guarded", while the default committer is the directory staging committer.

```
spark.hadoop.fs.s3a.committer.name=directory
spark.hadoop.fs.s3a.bucket.guarded.committer.name=magic
spark.hadoop.fs.s3a.bucket.guarded.committer.magic.enabled=true

```



## Troubleshooting




