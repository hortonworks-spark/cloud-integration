# Cloud Integration for Apache Spark

The [cloud-integration](https://github.com/hortonworks-spark/cloud-integration)
repository provides modules to improve Apache Spark's integration with cloud infrastructures.



## Module `spark-cloud-integration`

Classes and Tools to make Spark work better in-cloud

* Committer integration with the s3a committers.
* Proof of concept cloud-first distcp replacement.
* Serialization for Hadoop `Configuration`: class `ConfigSerDeser`. Use this
to get a configuration into an RDD method
* Trait `HConf` to manipulate the hadoop options in a spark config.
* Anything else which turns out to be useful.
* Variant of `FileInputStream` for cloud storage, `org.apache.spark.streaming.cloudera.CloudInputDStream`

See [Spark Cloud Integration](spark-cloud-integration/src/main/site/markdown/index.md)



## Module `cloud-examples`

This does the packaging/integration tests for Spark and cloud against AWS, Azure and openstack.

These are basic tests of the core functionality of I/O, streaming, and verify that
the commmitters work.

As well as running as unit tests, they have CLI entry points which can be used for scalable functional testing.


## Module `minimal-integration-test`

This is a minimal JAR for integration tests

Usage
```bash
spark-submit --class com.cloudera.spark.cloud.integration.Generator \
--master yarn \
--num-executors 2 \
--driver-memory 512m \
--executor-memory 512m \
--executor-cores 1 \
minimal-integration-test-1.0-SNAPSHOT.jar \
adl://example.azuredatalakestore.net/output/dest/1 \
2 2 15
```



