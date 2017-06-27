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

## <a name="testing"></a>Testing Spark's Cloud Integration

This project contains tests which can run against the object stores. These verify
functionality integration and performance.

### Example Configuration for Testing Cloud Data


The test runs need a configuration file to declare the (secret) bindings to the cloud infrastructure.
The configuration used is the Hadoop XML format, because it allows XInclude importing of
secrets kept out of any source tree.

The secret properties are defined using the Hadoop configuration option names, such as
`fs.s3a.access.key` and `fs.s3a.secret.key`

The file must be declared to the maven test run in the property `cloud.test.configuration.file`,
which can be done in the command line

```bash
mvn -T 1C test -Dcloud.test.configuration.file=../cloud.xml
```

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
    href="file:///home/developer/aws/auth-keys.xml"/>

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

<table class="table">
  <tr><th style="width:21%">Option</th><th>Meaning</th><th>Default</th></tr>
  <tr>
    <td><code>s3a.tests.enabled</code></td>
    <td>
    Execute tests using the S3A filesystem.
    </td>
    <td><code>false</code></td>
  </tr>
  <tr>
    <td><code>s3a.test.uri</code></td>
    <td>
    URI for S3A tests. Required if S3A tests are enabled.
    </td>
    <td><code></code></td>
  </tr>
  <tr>
    <td><code>s3a.test.csvfile.path</code></td>
    <td>
    Path to a (possibly encrypted) CSV file used in linecount tests.
    </td>
    <td><code></code>s3a://landsat-pds/scene_list.gz</td>
  </tr>
</table>

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

### Testing S3Guard consistency and committers


| Profile | Meaning | 
|--------|---------|
| `inconsistent` |  use the inconsisent S3 client | 
| `localdynamo` | S3Guard with a local database |
| `dynamo` | S3Guard with a remote database |
| `authoritative` | declare that the dynamo DB is considered authoritative |


The `inconsistent` profile enables the inconsistent listing AWS client. Any failing test implies the code being tested (or the test probes) do not work with an inconsistent FS.
As the test probes are meant to be resilient here, these failures are *probably* in the production codde.


```bash
mvn test  -Dcloud.test.configuration.file=../cloud-test-configs/s3a.xml   -DwildcardSuites=com.hortonworks.spark.cloud.s3 -Dinconsistent
```

Enable S3guard against a DynamoDBLocal datastore.

## Azure Test Options


<table class="table">
  <tr><th style="width:21%">Option</th><th>Meaning</th><th>Default</th></tr>
  <tr>
    <td><code>azure.tests.enabled</code></td>
    <td>
    Execute tests using the Azure WASB filesystem
    </td>
    <td><code>false</code></td>
  </tr>
  <tr>
    <td><code>azure.test.uri</code></td>
    <td>
    URI for Azure WASB tests. Required if Azure tests are enabled.
    </td>
    <td></td>
  </tr>
</table>


## Running a Single Test Case

Each cloud test takes time, especially if the tests are being run outside of the
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

```bash
mvn -T 1C  test -Dcloud.test.configuration.file=/home/developer/aws/cloud.xml -Dsuites=com.hortonworks.spark.cloud.s3.S3aIOSuite
```

If the test configuration in `/home/developer/aws/cloud.xml` does not have the property
`s3a.tests.enabled` set to `true`, the S3a suites are not enabled.
The named test suite will be skipped and a message logged to highlight this.

A single test can be explicitly run by including the key in the `suites` property
after the suite name

```bash
mvn -T 1C  test -Dcloud.test.configuration.file=/home/developer/aws/cloud.xml '-Dsuites=com.hortonworks.spark.cloud.s3.S3ABasicIOSuite FileOutput'
```

This will run all tests in the `S3ABasicIOSuite` suite whose name contains the string `FileOutput`;
here just one test. Again, the test will be skipped if the `cloud.xml` configuration file does
not enable s3a tests.

To run all tests of a specific infrastructure, use the `wildcardSuites` property to list the package
under which all test suites should be executed.

```
mvn -T 1C  test -Dcloud.test.configuration.file=/home/developer/aws/cloud.xml `-DwildcardSuites=com.hortonworks.spark.cloud`
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
| `com.hortonworks.spark.cloud.examples.CloudDataFrames` | `<dest> [<rows>]` | Dataframe IO across multiple formats
| `com.hortonworks.spark.cloud.s3.examples.S3LineCount` | `[<source>] [<dest>]` | S3A specific: count lines on a file, optionally write back.

## Best Practices for Adding a New Test

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

## Best Practices for Adding a New Test Suite

1. Extend `CloudSuite`
1. Have an `after {}` clause which cleans up all object stores —this keeps costs down.
1. Do not assume that any test has exclusive access to any part of an object store other
than the specific test directory. This is critical to support parallel test execution.
1. Share setup costs across test cases, especially for slow directory/file setup operations.
1. If extra conditions are needed before a test suite can be executed, override the `enabled` method
to probe for the extra conditions being met.

## Keeping Test Costs Down

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

## Test Profiles


### Scale tests

```
-Dscale
```

Enables the full test suite including bigger and slower tests. 


### Staging Committer 

```
-Dstaging
```

### Directory Staging Committer 

```
-Ddirectory
```

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

### Inconsistent 

```
-Dinconsistent
```


Switches to inconsisent listing for S3A paths with `DELAY_LISTING_ME` in
the path name. This is for both adding and deleting files. 

The files themselves have the consistency offered by S3; `HEAD`, `GET`
and `DELETE` will not be made any *worse*, and, in tests, usually
appear consistent.

### `localdynamo`: S3guard with a local dynamo DB instance


```
-Dlocaldynamo
```

This can be used to demonstrate how S3Guard addresses inconsistency in
listings


```
-Dinconsistent -Dlocaldynamo -Dauthoritative
```


### `dynamo`: S3guard with a real dynamo DB connection

Enables dynamo

```
-Ddynamo
```

This can be used to demonstrate how S3Guard addresses inconsistency in
listings

```
-Dinconsistent -Ddynamo -Dauthoritative
```


### `authoritative`: Make the s3guard dynamo DB authoritative

This declares that the DynamoDB with S3Guard metadata is the source of
truth regarding directory listings, so operations do not need to look at
S3 itself. This is faster, but 


```
-Dauthoritative
```




### Failing `failing`

This is a fairly useless profile right now, as it injects failures into
S3A client setup. It exists more to verify property passdown than
do any useful test coverage.

```
-Dfailing
```

