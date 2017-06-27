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

# Cloud Examples

This modules contains tests which verify Apache Spark's
integration and performance with object stores, specifically Amazon S3, Azure and Openstack.
The underlying client libraries are part of Apache Hadoop —thus these tests can act as integration
and regression tests for changes in the Hadoop codebase.

The generated artifact contains a test suite, `com.hortonworks.spark.cloud.CloudSuite`,
which can be used as a base class for integration tests.

The artifact is *not* indended to be used in production code, purely in
`test` scope.

## Building a local version of Spark to test with

This module needs a version of Spark with the `spark-cloud` module with it; at the time of
writing this is not yet in ASF spark.

1. Optional: Build any local Hadoop version which you want to use in these integration tests, 
 using `mvn install -DskipTests` in the `hadoop-trunk` dir. For example: `2.8.0`

1. add repository `https://github.com/steveloughran/spark.git`
1. check out branch `features/SPARK-7481-cloud` from the `steveloughran` repo

1. Build Spark with the version of Hadoop you built locally.

        mvn install -DskipTests -Pyarn,hive,hadoop-2.7,cloud -Dhadoop.version=2.8.0 

    This installs the spark JARs and POMs into the local maven repsitory, where they can be
    used until midnight. You will need to repeat the spark and hadoop builds every morning.
    Tip: if you are new to building spark, you can speed up your life by installing zinc via
    apt-get, yum or homebrew, then launch it for background compilation with: `zinc -start`
     
    Once this operation is complete, you can run tests with the spark version set on this build
    `-Dspark.version=2.x.y` ; the default, `2.1.0-SNAPSHOT` is that of spark's `master` branch
    at the time of writing.
    
1. To do a full spark installation:

        dev/make-distribution.sh -Pyarn,hive,hadoop-2.7,cloud -Dhadoop.version=2.8.0


## Building a compatible Hadoop version


If you are working with Hadoop 2.7-2.8 you can use the shipping versions.

To work with Hadoop trunk or s3guard branches, you need to build a local version
of Hadoop with its internal version information set to 2.11, so that the hive 1.2.1
JAR in Spark doesn't refuse to work with Hadoop


```
mvn -T1C install -DskipTests -DskipShade -Ddeclared.hadoop.version=2.11 

```

You may also need to make sure the versions of Guava used are consistent.
That is left as an exercise for the reader.

There is a test which explicitly checks the Hadoop version matches that passed
in as `hadoop.required.version`; it implicitly checks that Guava is compatible
across artifacts.


```
mvn -T1C test -DwildcardSuites=com.hortonworks.spark.cloud.common.HadoopVersionSuite  -Phadoop-3.0
```

## Test Configuration


The tests need a configuration file to declare the (secret) bindings to the cloud infrastructure.
The configuration used is the Hadoop XML format, because it allows XInclude importing of
secrets kept out of any source tree.

The secret properties are defined using the Hadoop configuration option names, such as
`fs.s3a.access.key` and `fs.s3a.secret.key`

The file must be declared to the maven test run in the property `cloud.test.configuration.file`,
which can be done in the command line

```bash
mvn -T1C test -Dcloud.test.configuration.file=../cloud.xml
```

As this project looks for, and reads in any `build.properties` file in the project
directory, the path *may* be declarable in that file:

```properties
cloud.test.configuration.file=/home/developer/aws/cloud.xml
```

However, attempts to do this appear to fail. Help welcome.


*Important*: keep all credentials out of SCM-managed repositories. Even if `.gitignore`
or equivalent is used to exclude the file, they may unintenally get bundled and released
with an application. It is safest to keep the `cloud.xml` files out of the tree,
and keep the authentication secrets themselves in a single location for all applications
tested.

Here is an example XML file `/home/developer/aws/cloud.xml` for running the S3A and Azure tests,
referencing the secret credentials kept in the file `/home/hadoop/aws/auth-keys.xml`.

```xml
<configuration>
  <include xmlns="http://www.w3.org/2001/XInclude"
    href="//home/developer/aws/auth-keys.xml"/>

  <property>
    <name>s3a.tests.enabled</name>
    <value>true</value>
    <description>Flag to enable S3A tests</description>
  </property>

  <property>
    <name>s3a.test.uri</name>
    <value>s3a://testplan1</value>
    <description>S3A path to a bucket which the test runs are free to write, read and delete
    data.</description>
  </property>

  <property>
    <name>azure.tests.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>azure.test.uri</name>
    <value>wasb://MYCONTAINER@TESTACCOUNT.blob.core.windows.net</value>
  </property>

</configuration>
```

The configuration uses XInclude to pull in the secret credentials for the account
from the user's `/home/developer/.ssh/auth-keys.xml` file:

```xml
<configuration>
  <property>
    <name>fs.s3a.access.key</name>
    <value>USERKEY</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>SECRET_AWS_KEY</value>
  </property>
  <property>
    <name>fs.azure.account.key.TESTACCOUNT.blob.core.windows.net</name>
    <value>SECRET_AZURE_KEY</value>
  </property>
</configuration>
```

Splitting the secret values out of the other XML files allows for the other files to
be managed via SCM and/or shared, with reduced risk.

Note that the configuration file is used to define the entire Hadoop configuration used
within the Spark Context created; all options for the specific test filesystems may be
defined, such as endpoints and timeouts.

### S3A Options


| Option | Meaning | Default |
|--------|---------|---------|
| `s3a.tests.enabled` | Execute tests using the S3A filesystem | `false`|
| `s3a.test.csvfile.path` | Path to a read only CSV file used in performance tests. | `s3a://landsat-pds/scene_list.gz`|



When testing against Amazon S3, their [public datasets](https://aws.amazon.com/public-data-sets/)
are used.

The gzipped CSV file `s3a://landsat-pds/scene_list.gz` is used for testing line input and file IO;
the default is a 20+ MB file hosted by Amazon. This file is public and free for anyone to
access, making it convenient and cost effective.

The size and number of lines in this file increases over time;
the current size of the file can be measured through `curl`:

```bash
curl -I -X HEAD http://landsat-pds.s3.amazonaws.com/scene_list.gz
```

When testing against non-AWS infrastructure, an alternate file may be specified
in the option `s3a.test.csvfile.path`; with its endpoint set to that of the
S3 endpoint


```xml
  <property>
    <name>s3a.test.csvfile.path</name>
    <value>s3a://testdata/landsat.gz</value>
  </property>

  <property>
    <name>fs.s3a.endpoint</name>
    <value>s3server.example.org</value>
  </property>


```

When testing against an S3 instance which only supports the AWS V4 Authentication
API, such as Frankfurt and Seoul, the `fs.s3a.bucket.landsat-pds.endpoint` property must be set to that of
the specific location. Because the public landsat dataset is hosted in AWS US-East, it must retain
the original S3 endpoint. This is done by default, though it can also be set explicitly:


```xml
<property>
  <name>fs.s3a.endpoint</name>
  <value>s3.eu-central-1.amazonaws.com</value>
</property>

<property>
  <name>fs.s3a.bucket.landsat-pds.endpoint</name>
  <value>s3.amazonaws.com</value>
</property>
```

Finally, the CSV file tests can be skipped entirely by declaring the URL to be ""


```xml
<property>
  <name>s3a.test.csvfile.path</name>
  <value/>
</property>
```
### Azure Test Options

| Option | Meaning | Default |
|--------|---------|---------|
| `azure.tests.enabled` |Execute tests using the Azure WASB filesystem| `false`|
| `azure.test.uri` | URI for Azure WASB tests. Required if Azure tests are enabled.| |

## Running a Single Test Case

Each test takes time, especially if the tests are being run outside of the
infrastructure of the specific cloud infrastructure provider.
Accordingly, it is important to be able to work on a single test case at a time
when implementing or debugging a test.

Tests in a cloud suite must be conditional on the specific filesystem being available; every
test suite must implement a method `enabled: Boolean` to determine this. The tests are then
registered as "conditional tests" via the `ctest()` functino, which, takes a key,
a detailed description (this is included in logs), and the actual function to execute.

For example, here is the test `NewHadoopAPI`.

```scala

  ctest("NewHadoopAPI",
    "Use SparkContext.saveAsNewAPIHadoopFile() to save data to a file") {
    sc = new SparkContext("local", "test", newSparkConf())
    val numbers = sc.parallelize(1 to testEntryCount)
    val example1 = new Path(TestDir, "example1")
    saveAsTextFile(numbers, example1, sc.hadoopConfiguration)
  }
```

This test can be executed as part of the suite `S3aIOSuite`, by setting the `suites` maven property to the classname
of the test suite:

```
mvn -T1C test -Dcloud.test.configuration.file=/home/developer/aws/cloud.xml -Dsuites=com.hortonworks.sparkspark.cloud.s3.S3AIOSuite
```

If the test configuration in `/home/developer/aws/cloud.xml` does not have the property
`s3a.tests.enabled` set to `true`, the S3a suites are not enabled.
The named test suite will be skipped and a message logged to highlight this.

A single test can be explicitly run by including the key in the `suites` property
after the suite name

```
mvn -T1C test -Dcloud.test.configuration.file=/home/developer/aws/cloud.xml `-Dsuites=com.hortonworks.spark.cloud.s3.S3AIOSuite NewHadoopAPI`
```

This will run all tests in the `S3AIOSuite` suite whose name contains the string `NewHadoopAPI`;
here just one test. Again, the test will be skipped if the `cloud.xml` configuration file does
not enable s3a tests.

To run all tests of a specific infrastructure, use the `wildcardSuites` property to list the package
under which all test suites should be executed.

```
mvn -T1C test -Dcloud.test.configuration.file=/home/developer/aws/cloud.xml `-DwildcardSuites=com.hortonworks.spark.cloud.s3`
```

Note that an absolute path is used to refer to the test configuration file in these examples.
If a relative path is supplied, it must be relative to the project base, *not the cloud module*.

## Integration tests

The module includes a set of tests which work as integration tests, as well as unit tests. These
can be executed against live spark clusters, and can be configured to scale up, so testing
scalability.

| job | arguments | test |
|------|----------|------|
| `com.hortonworks.spark.cloud.CloudFileGenerator` | `<dest> <months> <files-per-month> <row-count>` | Parallel generation of files |
| `com.hortonworks.spark.cloud.CloudStreaming` | `<dest> [<rows>]` | Verifies that file streaming works with object store |
| `com.hortonworks.spark.cloud.CloudDataFrames` | `<dest> [<rows>]` | Dataframe IO across multiple formats
| `com.hortonworks.spark.cloud.S3ALineCount` | `[<source>] [<dest>]` | S3A specific: count lines on a file, optionally write back.

## Best Practices

### Best Practices for Adding a New Test

1. Use `ctest()` to define a test case conditional on the suite being enabled.
1. Keep the test time down through small values such as: numbers of files, dataset sizes, operations.
Avoid slow operations such as: globbing & listing files
1. Support a command line entry point for integration tests —and allow such tests to scale up
though command line arguments.
1. Give the test a unique name which can be used to explicitly execute it from the build via the `suite` property.
1. Give the test a meaningful description for logs and test reports.
1. Test against multiple infrastructure instances.
1. Allow for eventual consistency of deletion and list operations by using `eventually()` to
wait for object store changes to become visible.
1. Have a long enough timeout that remote tests over slower connections will not timeout.

### Best Practices for Adding a New Test Suite

1. Extend `CloudSuite`
1. Have an `after {}` clause which cleans up all object stores —this keeps costs down.
1. Do not assume that any test has exclusive access to any part of an object store other
than the specific test directory. This is critical to support parallel test execution.
1. Share setup costs across test cases, especially for slow directory/file setup operations.
1. If extra conditions are needed before a test suite can be executed, override the `enabled` method
to probe for the extra conditions being met.

### Keeping Test Costs Down

Object stores incur charges for storage and for GET operations out of the datacenter where
the data is stored.

The tests try to keep costs down by not working with large amounts of data, and by deleting
all data on teardown. If a test run is aborted, data may be retained on the test filesystem.
While the charges should only be a small amount, period purges of the bucket will keep costs down.

Rerunning the tests to completion again should achieve this.

The large dataset tests read in public data, so storage and bandwidth costs
are incurred by Amazon and other cloud storage providers themselves.

### Keeping Credentials Safe in Testing

It is critical that the credentials used to access object stores are kept secret. Not only can
they be abused to run up compute charges, they can be used to read and alter private data.

1. Keep the XML Configuration file with any secrets in a secure part of your filesystem.
1. When using Hadoop 2.8+, consider using Hadoop credential files to store secrets, referencing
these files in the relevant id/secret properties of the XML configuration file.
1. Do not execute object store tests as part of automated CI/Jenkins builds, unless the secrets
are not senstitive -for example, they refer to in-house (test) object stores, authentication is
done via IAM EC2 VM credentials, or the credentials are short-lived AWS STS-issued credentials
with a lifespan of minutes and access only to transient test buckets.

## Working with Hadoop 3.x

Hive doesn't know how to handle Hadoop version 3; all the dataframe tests will fail against
Hadoop 3.x binaries.

There's a patch in some of the Hadoop branches to let you build hadoop with a different
declare version than that in the POM. All the jars will have the POM version, but
the version returned by calls to `VersionInfo` will return the value set in `declared.hadoop.version`.


```bash
mvn -T1C install -DskipTests -DskipShade -Ddeclared.hadoop.version=2.11
```

1. the `-DskipShade` option skips the JAR shading process, which takes a long time and is not needed here.
1. You cannot use a JAR built this way to work with HDFS; the version numbers just confuse things
In particular: *Do not try to run Datanodes or Namenodes with a JAR numbered this way.*

### Working with Private repositories and older artifact names

(This is primarily of interest to colleagues testing releases with this code.)

1. To build with a different version of spark, define it in `spark.version`
1. To use the older artifact name, `spark-cloud`, define `spark.cloud.jar` to this name.
1. To use a different repository for artifacts, redefine `central.repo`
1. If the spark cloud POM doesn't declare the Apache httpcomponent versions, you need to
explicitly list them through `-Pdeclare-http-components`

Example:

```bash
mvn -T1C test -T 1C -Dspark.version=2.0.0.2.5.0.14-5 \
  -Dspark.cloud.jar=spark-cloud \
  -Dcentral.repo=http://PRIVATE-REPO/nexus/content/ 
```

You can of course also have private profiles in ` ~/.m2/settings.xml`.

This is the best way to have consistent private repositories across multiple builds

### Example test runs


Run the s3a tests (assuming the s3a.xml file contained/referenced the s3a binding information),
with the Spark 2.3.0-SNAPSHOT binaries.

```bash
mvn -T1C test -Dcloud.test.configuration.file=../../cloud-test-configs/s3a.xml  -Dspark.version=2.3.0-SNAPSHOT
```
