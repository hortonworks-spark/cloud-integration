# Cloud Integration for Apache Spark

The [cloud-integration](https://github.com/hortonworks-spark/cloud-integration)
repository provides modules to improve Apache Spark's integration with cloud infrastructures.



## spark-cloud-integration

Classes and Tools to make Spark work better in-cloud

* Committer integration with the s3a committers
* Proof of concept cloud-first distcp replacement
* Anything else which turns out to be useful
* Variant of FileInputStream for cloud storage, `org.apache.spark.streaming.hortonworks.CloudInputDStream`

See [Spark Cloud Integration](spark-cloud-integration/src/main/site/markdown/index.md)



## cloud-examples

This does the packaging/integration tests for Spark and cloud against AWS, Azure and openstack.

These are basic tests of the core functionality of I/O, streaming, and verify that
the commmitters work in the presence of inconsistent object storage
As well as running as unit tests, they have CLI entry points which can be used for scalable functional testing.


