/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package com.cloudera.spark.cloud.s3

import scala.util.matching.Regex
/*
83c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:53:49 +0000] 109.157.193.10 arn:aws:sts::152813717728:assumed-role/stevel-assumed-role/test B5CD03A97884701C REST.HEAD.OBJECT fork-0001/test/restricted/__magic/magic2 "HEAD /fork-0001/test/restricted/__magic/magic2 HTTP/1.1" 404 NoSuchKey 311 - 10 - "https://hadoop.apache.org/audit/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.53.46/s/00000005/op/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.53.46?principal=stevel&op=op_rename&path=s3a://stevel-london/fork-0001/test/restricted/__magic/testMarkerFileRename&path2=s3a://stevel-london/fork-0001/test/restricted/__magic/magic2" "Hadoop 3.4.0-SNAPSHOT, aws-sdk-java/1.11.901 Mac_OS_X/10.15.7 OpenJDK_64-Bit_Server_VM/25.252-b09 java/1.8.0_252 vendor/AdoptOpenJDK" - Mv4hQWa0/RaBNZjvczmMMtbeNdEmnKHwWyqCMR4DlA0leAIDcKDtd5HND4g0EBygr4qjZROJXo4= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:53:50 +0000] 109.157.193.10 arn:aws:sts::152813717728:assumed-role/stevel-assumed-role/test 012F813E53259B94 REST.DELETE.UPLOAD fork-0001/test/restricted/testMarkerFileRename "DELETE /fork-0001/test/restricted/testMarkerFileRename?uploadId=WK2d8rzLa9v4.vHP_mwX1omWGIdwZdn5O_DflVJnkxZ.sPeD4YUuMa4GaRKVaWlyi44p9nY7bphauzXrdkWOYBco2x.qhtQdXI_6QYUuYxnf2FnHgkgTQ0UIrVjYIda.2UN3wOMAAYnl02MozGjgEw-- HTTP/1.1" 204 - - - 18 17 "https://hadoop.apache.org/audit/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.53.46/s/00000007/op/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.53.46?principal=stevel&op=committer_commit_job&path=/" "Hadoop 3.4.0-SNAPSHOT, aws-sdk-java/1.11.901 Mac_OS_X/10.15.7 OpenJDK_64-Bit_Server_VM/25.252-b09 java/1.8.0_252 vendor/AdoptOpenJDK" - /j8lCVIwOUE2cSiEynqkG+2glhbETAWzWTDZd4w/2cQKOjd8epM4aEk7BjY0oykVZOBkhu4rjGE= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:53:50 +0000] 109.157.193.10 arn:aws:sts::152813717728:assumed-role/stevel-assumed-role/test 8445EBDA588DEFEB REST.GET.UPLOADS - "GET /?uploads&prefix=fork-0001%2Ftest%2Frestricted%2F HTTP/1.1" 200 - 1169 - 15 13 "https://hadoop.apache.org/audit/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.53.46/s/00000007/op/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.53.46?principal=stevel&op=committer_commit_job&path=/" "Hadoop 3.4.0-SNAPSHOT, aws-sdk-java/1.11.901 Mac_OS_X/10.15.7 OpenJDK_64-Bit_Server_VM/25.252-b09 java/1.8.0_252 vendor/AdoptOpenJDK" - k4eGhwn+KLmEkznRwWxO2klembT3P16HMrIc6RsnFxOthiuAgFDU8SHYL31zP7nVOGko6mF1zJI= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:53:53 +0000] 109.157.193.10 arn:aws:iam::152813717728:user/stevel-dev 0966E574330716D9 REST.PUT.OBJECT fork-0004/test/eventually-reopen.dat "PUT /fork-0004/test/eventually-reopen.dat HTTP/1.1" 200 - - 7 49 23 "https://hadoop.apache.org/audit/dbfdceb7-a11a-4c39-91a1-49f286bf5a36-14.53.47/s/0000000d/op/dbfdceb7-a11a-4c39-91a1-49f286bf5a36-14.53.47?principal=stevel&op=op_create&path=s3a://stevel-london/fork-0004/test/eventually-reopen.dat" "Hadoop 3.4.0-SNAPSHOT, aws-sdk-java/1.11.901 Mac_OS_X/10.15.7 OpenJDK_64-Bit_Server_VM/25.252-b09 java/1.8.0_252 vendor/AdoptOpenJDK" - FH0SAldavv4Q3TavjvSVnbdvJ0mxPcIn9M40wjYI3EHwE+v2dDaEowgHUj5Y2AEcFTmzz6LRx0w= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:53:53 +0000] 109.157.193.10 arn:aws:iam::152813717728:user/stevel-dev D630F8F6B949DA00 BATCH.DELETE.OBJECT fork-0004/ - 204 - - - - - - - - WVzx2GM3UeoXx11TVb+v1u1reJl+ahOiJApP9NDIHWBQPF87OXQyKKc/31NGXPNXLSEigGJZleg= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:53:53 +0000] 109.157.193.10 arn:aws:iam::152813717728:user/stevel-dev D630F8F6B949DA00 BATCH.DELETE.OBJECT fork-0004/test/ - 204 - - - - - - - - WVzx2GM3UeoXx11TVb+v1u1reJl+ahOiJApP9NDIHWBQPF87OXQyKKc/31NGXPNXLSEigGJZleg= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:53:53 +0000] 109.157.193.10 arn:aws:iam::152813717728:user/stevel-dev D630F8F6B949DA00 REST.POST.MULTI_OBJECT_DELETE - "POST /?delete HTTP/1.1" 200 - 116 - 73 - "https://hadoop.apache.org/audit/dbfdceb7-a11a-4c39-91a1-49f286bf5a36-14.53.47/s/0000000d/op/dbfdceb7-a11a-4c39-91a1-49f286bf5a36-14.53.47?principal=stevel&op=op_create&path=s3a://stevel-london/fork-0004/test/eventually-reopen.dat" "Hadoop 3.4.0-SNAPSHOT, aws-sdk-java/1.11.901 Mac_OS_X/10.15.7 OpenJDK_64-Bit_Server_VM/25.252-b09 java/1.8.0_252 vendor/AdoptOpenJDK" - WVzx2GM3UeoXx11TVb+v1u1reJl+ahOiJApP9NDIHWBQPF87OXQyKKc/31NGXPNXLSEigGJZleg= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:54:09 +0000] 109.157.193.10 arn:aws:iam::152813717728:user/stevel-dev 039450FC77B1F9B1 BATCH.DELETE.OBJECT fork-0001/test/ - 204 - - - - - - - - do8tYo9l/uQgU21o7i6s5vAWN9yjKUBqHtPGP9j0rCjnyfybLVPVcau5baOVyW8KvmswujgyG2o= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:54:09 +0000] 109.157.193.10 arn:aws:iam::152813717728:user/stevel-dev 039450FC77B1F9B1 REST.POST.MULTI_OBJECT_DELETE - "POST /?delete HTTP/1.1" 200 - 116 - 91 - "https://hadoop.apache.org/audit/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.54.02/s/00000005/op/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.54.02?principal=stevel&op=op_delete&path=s3a://stevel-london/fork-0001/test" "Hadoop 3.4.0-SNAPSHOT, aws-sdk-java/1.11.901 Mac_OS_X/10.15.7 OpenJDK_64-Bit_Server_VM/25.252-b09 java/1.8.0_252 vendor/AdoptOpenJDK" - do8tYo9l/uQgU21o7i6s5vAWN9yjKUBqHtPGP9j0rCjnyfybLVPVcau5baOVyW8KvmswujgyG2o= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2

 */
/**
 * see https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
 */
object S3LogRecordParser {

  /**
   * Regular expression from the AWS Documentation
   */
  val RECORD: Regex =
        // upo to referrer and
       ("([^ ]*)" +       // owner
        " ([^ ]*)" +      // bucket
        " \\[(.*?)\\]" +  // time
        " ([^ ]*)" +      // remote ip
        " ([^ ]*)" +      // requester
        " ([^ ]*)" +      // requestid
        " ([^ ]*)" +      // operation
        " ([^ ]*)" +      // key
        " (\"[^\"]*\"|-)" + // request_uri
        " (-|[0-9]*)" +   // httpstatus
        " ([^ ]*)" +      // errorcode
        " ([^ ]*)" +      // bytessent
        " ([^ ]*)" +      // objectsize
        " ([^ ]*)" +      // totaltime
        " ([^ ]*)" +      // turnaroundtime
        " ([^ ]*)" +      // referrer
        " (\"[^\"]*\"|-)" +  // UA
        " ([^ ]*)" +      // version
         "([^ ]*)" +      // version
        " ([^ ]*)" +      // host ID
        " ([^ ]*)" +      // sigv
        " ([^ ]*)" +      // ciphersuite
        " ([^ ]*)" +      // authtype
        " ([^ ]*))?.*$").r;
  /*
  CREATE EXTERNAL TABLE `s3_access_logs_db.mybucket_logs`(
  `bucketowner` STRING,
  `bucket_name` STRING,
  `requestdatetime` STRING,
  `remoteip` STRING,
  `requester` STRING,
  `requestid` STRING,
  `operation` STRING,
  `key` STRING,
  `request_uri` STRING,
  `httpstatus` STRING,
  `errorcode` STRING,
  `bytessent` BIGINT,
  `objectsize` BIGINT,
  `totaltime` STRING,
  `turnaroundtime` STRING,
  `referrer` STRING,
  `useragent` STRING,
  `versionid` STRING,
  `hostid` STRING,
  `sigv` STRING,
  `ciphersuite` STRING,
  `authtype` STRING,
  `endpoint` STRING,
  `tlsversion` STRING)

   */

  def parse(source: String): LogRecord = {

    val m = RECORD.findAllIn(source)

    LogRecord(
      bucketowner = m.group(1),
      bucket_name = m.group(2),
      requestdatetime = m.group(3),
      remoteip = m.group(4),
      requester = m.group(5),
      requestid = m.group(6),
      operation = m.group(7),
      key = m.group(8),
      request_uri = m.group(9),
      httpstatus = m.group(10),
      errorcode = m.group(11),
      bytessent= m.group(12).toLong,
      objectsize = m.group(13).toLong,
      totaltime = m.group(14),
      turnaroundtime = m.group(15),
      referrer = m.group(16),
      useragent = m.group(17),
      versionid = m.group(18),
      hostid = m.group(19),
      sigv = m.group(20),
      ciphersuite = m.group(21),
      authtype = m.group(22),
      endpoint = m.group(23),
      tlsversion = m.group(24)
    )

  }

}

/**
 * Record
 */
case class LogRecord(
  bucketowner: String,
  bucket_name: String,
  requestdatetime: String,
  remoteip: String,
  requester: String,
  requestid: String,
  operation: String,
  key: String,
  request_uri: String,
  httpstatus: String,
  errorcode: String,
  bytessent: Long,
  objectsize: Long,
  totaltime: String,
  turnaroundtime: String,
  referrer: String,
  useragent: String,
  versionid: String,
  hostid: String,
  sigv: String,
  ciphersuite: String,
  authtype: String,
  endpoint: String,
  tlsversion: String) {

  override def toString: String =
    s"$operation /$bucket_name/$key $httpstatus $errorcode $bytessent $requestdatetime"
}