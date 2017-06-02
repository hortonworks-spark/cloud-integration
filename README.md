# Cloud Integration for Apache Spark

The [cloud-integration](https://github.com/hortonworks-spark/cloud-integration)
repository provides modules to improve Apache Spark's integration with cloud infrastructures.



## cloud-committer

Support for the S3guard committers in Apache Spark.

See [Cloud Committer](cloud-committer/src/main/site/markdown/index.md)

## cloud-examples

This does the packaging/integration tests for Spark and cloud against AWS, Azure and openstack.

These are basic tests of the core functionality of I/O, streaming, and verify that
the commmitters work in the presence of inconsistent object storage
As well as running as unit tests, they have CLI entry points which can be used for scalable functional testing.




