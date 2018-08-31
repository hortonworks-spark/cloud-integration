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

## Testing Spark Integration with cloud storage through Hadoop's Cloud connectors

This project contains tests designed to verify that Spark operations work against cloud stores through the hadoop filesystem APIs. None of the tests are particularly complex: they are designed to verify that the stores work as expected, and are not tests of Spark itself.

Test categories include

* Basic IO operations
* File IO performance on opened files
* Using object stores as a destination of RDD writes, and a followon source of reads
* Store-specific committers
* Dataframes
* Basic scale tests.


## Setting up for the tests


### Binding to the object stores


The testss need a configuration file to declare the (secret) bindings to the cloud infrastructures.
The configuration used is the Hadoop XML format, because it allows XInclude importing of
secrets kept out of any source tree.

The secret properties are defined using the Hadoop configuration option names, such as
`fs.s3a.access.key` and `fs.s3a.secret.key`

The file must be declared to the maven test run in the property `cloud.test.configuration.file`,
which can be done in the command line

```bash
mvn -T 1C test -Dcloud.test.configuration.file=/home/alice/cloud-test-configs/s3a.xml 
```

This is easiest if you set up an environment variable pointing to the file and refer to it on the command line.

```bash
export conf="-Dcloud.test.configuration.file=/home/alice/dev/cloud.xml"
mvn test -T 1C $CONF -DwildcardSuites=com.hortonworks.spark.cloud.s3.S3AConsistencySuite
````

Or for fish users (here with the hdp3 and scale profiles )
```bash
set -gx conf "-Dcloud.test.configuration.file=/home/alice/dev/cloud.xml"
mvn test -T 1C -Phdp3,scale $CONF -DwildcardSuites=com.hortonworks.spark.cloud.s3.S3AConsistencySuite
````




### Security


It is critical that the credentials used to access object stores are kept secret. Not only can
they be abused to run up compute charges, they can be used to read and alter private data.

1. Keep the XML Configuration file with any secrets in a secure part of your filesystem.
1. When using Hadoop 2.8+, consider using Hadoop credential files to store secrets, referencing
these files in the relevant id/secret properties of the XML configuration file.
1. Do not execute object store tests as part of automated CI/Jenkins builds, unless the secrets
are not senstitive -for example, they refer to in-house (test) object stores, authentication is
done via IAM EC2 VM credentials, or the credentials are short-lived AWS STS-issued credentials
with a lifespan of minutes and access only to transient test buckets.
1. Do not keep the credential files in SCM-managed directories to avoid accidentally checking thenm in.
It is safest to keep the `cloud.xml` files out of the tree,
and keep the authentication secrets themselves in a single location for all applications
tested.

Here is an example XML file `/home/alice/dev/cloud.xml` for running the S3A and Azure tests,
referencing the secret credentials kept in the file `/home/alice/secret/auth-keys.xml`.

```xml
<configuration>
  <include xmlns="http://www.w3.org/2001/XInclude"
    href="file:///home/alice/secret/auth-keys.xml"/>

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
  
  <property>
    <name>scale.tests.enabled.</name>
    <value>true</value>
  </property>

</configuration>
```

The configuration uses XInclude to pull in the secret credentials for the account
from the user's `/home/alice/secret/auth-keys.xml` file:

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
   ...
</configuration>
```

Splitting the secret values out of the other XML files allows for the other files to
be managed via SCM and/or shared, with reduced risk.

Note that the configuration file is used to define the entire Hadoop configuration used
within the Spark Context created; all options for the specific test filesystems may be
defined, such as endpoints and timeouts.


### Azure WASB Test Options

For azur wasb, enable the tests and provider a test URL. Your login credentials are also required.

```xml
  <property>
    <name>azure.tests.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>azure.test.uri</name>
    <value>wasb://USER@CONTAINER.blob.core.windows.net</value>
  </property>
```

### Azure ADL test options

Along with the normal `fs.adl` login options enabme the tests and provide a URI for testing.

```xml
  <property>
    <name>adl.tests.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>adl.test.uri</name>
    <value>${test.fs.adl.name}</value>
  </property>
````


### S3 Test Options

There are more of these than the rest because there are more complex tests against S3.

```xml

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

  <!-- only needed when testing against private S3 endpoints -->
  <property>
    <name>s3a.test.csvfile.path</name>
    <value>s3a://landsat-pds/scene_list.gz</value>
    <description>Path to a (possibly encrypted) CSV file used in linecount tests.</description>
  </property>

  ```


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
S3 endpoint using the S3a per-bucket configuration mechanism.


```xml
<property>
  <name>s3a.test.csvfile.path</name>
  <value>s3a://testdata/landsat.gz</value>
</property>

<property>
  <name>fs.s3a.endpoint</name>
  <value>s3server.example.org</value>
</property>

<property>
  <name>fs.s3a.bucket.testdata.endpoint</name>
  <value>${fs.s3a.endpoint}</value>
</property>
```

When testing against an S3 instance which only supports the AWS V4 Authentication
API, such as Frankfurt and Seoul, the `fs.s3a.endpoint` property must be set to that of
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

If you are testing with Hadoop 2.8+ you can also use the per-bucket options
to set the locations. This is already done in `rc/test/resources/core-site.xml`

Finally, the CSV file tests can be skipped entirely by declaring the URL to be ""


```xml
<property>
  <name>s3a.test.csvfile.path</name>
  <value/>
</property>
```


## Building the Tests


In the base directory of the project (`cloud-integration`)

```bash
mvn install -DskipTests
```


You can explicitly choose the spark version

```bash
mvn install -DskipTests -Dspark.version=2.3.1-SNAPSHOT
```

If you do this, remember to declare it on all test runs


## Running all the tests

Change to the subdirectory `cloud-examples` 

Assuming the `CONF` environment variable has been set up to point to the configuration file

```bash
mvn test $CONF -Dscale
```

Otherwise, explicitly invoke it. Here is an e
```bash
mvn test -Dcloud.test.configuration.file=../private/cloud.xml -Dscale -Dspark.version=2.3.1-SNAPSHOT
```

You can just run this project from the base directory

```
mvt $CONF --pl cloud-examples
```

Use absolute paths for referencing the XML file if you do this.



## Running a Single Test Case

Each cloud test takes time, especially if the tests are being run outside of the
infrastructure of the specific cloud infrastructure provider.
Accordingly, it is useful to be able to work on a single test case at a time
when implementing or debugging a test.

This test can be executed as part of the suite `S3aIOSuite`, by setting the `suites` maven property to the classname
of the test suite:

```bash
mvn test $CONF -Dsuites=com.hortonworks.spark.cloud.s3.S3aIOSuite
```

If the test configuration in `/home/developer/aws/cloud.xml` does not have the property
`s3a.tests.enabled` set to `true`, the S3a suites are not enabled.
The named test suite will be skipped and a message logged to highlight this.

A single test can be explicitly run by including the key in the `suites` property
after the suite name

```bash
mvn test $CONF '-Dsuites=com.hortonworks.spark.cloud.s3.S3ABasicIOSuite FileOutput'
```

This will run all tests in the `S3ABasicIOSuite` suite whose name contains the string `FileOutput`;
here just one test. Again, the test will be skipped if the `cloud.xml` configuration file does
not enable s3a tests.

To run all tests of a specific infrastructure, use the `wildcardSuites` property to list the package
under which all test suites should be executed.

```bash
mvn test $CONF  `-DwildcardSuites=com.hortonworks.spark.cloud`
```

Note that an absolute path is used to refer to the test configuration file in these examples.
If a relative path is supplied, it must be relative to the project base, *not the cloud module*.

# Integration tests

The module includes a set of tests which work as integration tests, as well as unit tests. These
can be executed against live spark clusters, and can be configured to scale up, so testing
scalability.

| job | arguments | test |
|------|----------|------|
| `com.hortonworks.spark.cloud.examples.CloudFileGenerator` | `<dest> <months> <files-per-month> <row-count>` | Parallel generation of files |
| `com.hortonworks.spark.cloud.examples.CloudStreaming` | `<dest> [<rows>]` | Verifies that file streaming works with object store |
| `com.hortonworks.spark.cloud.examples.CloudDataFrames` | `<dest> [<rows>]` | Dataframe IO across multiple formats  |
| `com.hortonworks.spark.cloud.s3.examples.S3LineCount` | `[<source>] [<dest>]` | S3A specific: count lines on a file, optionally write back. |




### Scale Tests

The scale tests are those which are considered slower and/or more costly,
and are best executed within the appropriate cloud infrastructure.

They are skipped unless the test run explicitly enables them

```bash
mvn test $CONF -Dscale
```


You can also enable this in the test configuration XML file by setting the
option `scale.tests.enabled` to true

The XML configuration option `scale.test.size.factor` can be used to scale up some of the tests in terms of
file size and number of operations; the default value is "100". Few tests use this.


### Disabling Spark & Hive tests


The tests which run the Hive services in the JVM seem to be the ones most prone to failing in bulk tests.
They do work when run individually.

To keep jenkins happy, they can be disabled in the bulk tests.

```bash
mvn test $CONF -DskipHiveTests
```

Setting the XML option `hive.tests.disabled` to true will have the same effect.

For completeness, the individual tests should still be executed in isolation.


## Analyzing the Results


### Generating the HTML test report

The tests will generate a set of XML reports in `target/surefire-reports/`,
files which can be converted by Jenkins into HTML reports.

The can also be converted at the command line:

```
mvn surefire-report:report-only 
```

This will generate the HTML report `target/site/surefire-report.html`

This appears a low quality HTML page because the site style sheets are missing
run `mvn -o site:site` to build the full site and set thing up.

### Interpreting Skipped Tests

A lot of tests will be reported as skipped.

This includes

* All tests for filesystems not enabled in the test run.
  Expect only those tests begining with the test stores to be tested (S3A, Azure, ADL,...).
* All files with scalatest tess in them but which are not actually instantiable test suites.
  Expect only files with the suffix `Suite` to be run.
* Tests in the `Relations` suite which don't work with the formats of the tested filesystem.
  Those tests are derived from local-fs tests in Spark's own codebase; they've
  been expanded rather than purged.
* Scale tests when the `-Dscale` option was not set on the maven command line, or
`scale.tests.enabled` set in the test configuration XML file.

If scale tests are desired: enable the  `-Dscale` option and rerun.


## Debugging Test Failures

The test runner can be configured to block awaiting a debugger to attach
to the JVM: use the `-Ddebug` command. 

```
-Ddebug
```

You can then connect to the process from a debugger.

(This works better than trying to run tests from within an IDE such as IntelliJ IDEA, which gets upset about
conflicting Guava versions on the classpath).



## Intermittent Test Failures

Here are some problems with test execution which are outstanding; seems to be related to the state of the JVM after
the previous test runs.


#### Spark Context Lifetimes in the JVM.

Scalatest doesn't support the one-JVM-per-test class feature of the JUnit test runners;
the tests do their best to clean up across each suite.

This seems to lead the problems running the relation suites, where when the
first one to finish closes the active spark context, so the successors fail.

At the same time, if that context is not closed, other tests fail. 


#### Observed inconsistency, such as files not being found

Turn S3Guard on for the tests, it's what it fixes.

The test setup code tries to allow for some delays in metadata listings, especially when deleting target directories.



### "Cannot call methods on a stopped SparkContext. "

This means that some other test has stopped the spark context.
Try running the test suite in isolation.


## Testing S3 SSE-KMS Encryption

A (small) test exists for encryption using SSE-KMS
, `com.hortonworks.spark.cloud.s3.S3AEncryptionSuite`.
For it to execute, the XML configuration file needs to contain two encryption
keys.

```xml
<property>
  <name>s3a.test.encryption.key.1</name>
  <value>>arn:aws:kms:eu-west-1:00000:key/11111</value>
</property>

<property>
  <name>s3a.test.encryption.key.2</name>
  <value>arn:aws:kms:eu-west-1:00000:key/22222</value>
</property>
```

These aren't very important tests; all they do is make sure that the encryption options are passed all the way down.





## Testing with S3Guard 


If the object store has S3Guard enabled, then you get the consistent view which it offers.

You can also turn it on just for these tests, possibly also with the authoritative option.


#### `dynamodb`: S3guard with a real dynamo DB connection

Enables dynamo

```
-Ddynamodb
```

This can be used to demonstrate how S3Guard addresses inconsistency in
listings

```
-Dinconsistent -Ddynamodb -Dauthoritative
```


#### `authoritative`: Make the s3guard dynamo DB authoritative

This declares that the DynamoDB with S3Guard metadata is the source of
truth regarding directory listings, so operations do not need to look at
S3 itself. This is faster, but 


```
-Ddynamodb -Dauthoritative
```

### Turning on Fault injection

S3A has a fault injection option; this can be enabled by configuring the S3A
client as covered in
[the "Testing S3A" document](https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/site/markdown/tools/hadoop-aws/testing.md#failure-injection)


```
-Dinconsistent
```


Switches to fault-injecting client with inconsistent listing for S3A paths
with `DELAY_LISTING_ME` in the path name. This is for both adding and deleting files. 

*Directories used for S3A tests are all under this path, to force the inconsistencies*

The files themselves have the consistency offered by S3; `HEAD`, `GET`
and `DELETE` will not be made any *worse*, and, in tests, usually
appear consistent.


## Changing the committer for test runs

The default committer is "directory". Currently the most of the tests set this up explicitly, though there is support for declaring
use of the alternative S3A committers -it's just disabled right now to avoid confusion.

### Partitioned Staging Committer 

```
-Dpartitioned
```

### Magic Committer 

Uses the magic committer. The FS must be configured with support for "magic" 
paths, recognising paths with the path element `__magic` as redirecting files
written underneath as uncommitted multipart PUT operations to magic directories.

```
-Dmagic
```

**Important** Use a dynamodb profile to enable consistent lookups. If you
enable inconsistent listings with `-Dinconsistent` the reason becomes obvious



## Adding New Tests

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

### Best Practices for Adding a  Test Case

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

### Best Practices for Adding a  Test Suite

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

