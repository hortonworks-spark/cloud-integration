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

import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.internal.Logging

/**
 * log parsing
 */
class TestLogParsing extends FunSuite with Logging with Matchers {

  val records ="""83c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:53:49 +0000] 109.157.193.10 arn:aws:sts::152813717728:assumed-role/stevel-assumed-role/test B5CD03A97884701C REST.HEAD.OBJECT fork-0001/test/restricted/__magic/magic2 "HEAD /fork-0001/test/restricted/__magic/magic2 HTTP/1.1" 404 NoSuchKey 311 - 10 - "https://hadoop.apache.org/audit/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.53.46/s/00000005/op/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.53.46?principal=stevel&op=op_rename&path=s3a://stevel-london/fork-0001/test/restricted/__magic/testMarkerFileRename&path2=s3a://stevel-london/fork-0001/test/restricted/__magic/magic2" "Hadoop 3.4.0-SNAPSHOT, aws-sdk-java/1.11.901 Mac_OS_X/10.15.7 OpenJDK_64-Bit_Server_VM/25.252-b09 java/1.8.0_252 vendor/AdoptOpenJDK" - Mv4hQWa0/RaBNZjvczmMMtbeNdEmnKHwWyqCMR4DlA0leAIDcKDtd5HND4g0EBygr4qjZROJXo4= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
      |183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:53:50 +0000] 109.157.193.10 arn:aws:sts::152813717728:assumed-role/stevel-assumed-role/test 012F813E53259B94 REST.DELETE.UPLOAD fork-0001/test/restricted/testMarkerFileRename "DELETE /fork-0001/test/restricted/testMarkerFileRename?uploadId=WK2d8rzLa9v4.vHP_mwX1omWGIdwZdn5O_DflVJnkxZ.sPeD4YUuMa4GaRKVaWlyi44p9nY7bphauzXrdkWOYBco2x.qhtQdXI_6QYUuYxnf2FnHgkgTQ0UIrVjYIda.2UN3wOMAAYnl02MozGjgEw-- HTTP/1.1" 204 - - - 18 17 "https://hadoop.apache.org/audit/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.53.46/s/00000007/op/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.53.46?principal=stevel&op=committer_commit_job&path=/" "Hadoop 3.4.0-SNAPSHOT, aws-sdk-java/1.11.901 Mac_OS_X/10.15.7 OpenJDK_64-Bit_Server_VM/25.252-b09 java/1.8.0_252 vendor/AdoptOpenJDK" - /j8lCVIwOUE2cSiEynqkG+2glhbETAWzWTDZd4w/2cQKOjd8epM4aEk7BjY0oykVZOBkhu4rjGE= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
      |183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:53:50 +0000] 109.157.193.10 arn:aws:sts::152813717728:assumed-role/stevel-assumed-role/test 8445EBDA588DEFEB REST.GET.UPLOADS - "GET /?uploads&prefix=fork-0001%2Ftest%2Frestricted%2F HTTP/1.1" 200 - 1169 - 15 13 "https://hadoop.apache.org/audit/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.53.46/s/00000007/op/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.53.46?principal=stevel&op=committer_commit_job&path=/" "Hadoop 3.4.0-SNAPSHOT, aws-sdk-java/1.11.901 Mac_OS_X/10.15.7 OpenJDK_64-Bit_Server_VM/25.252-b09 java/1.8.0_252 vendor/AdoptOpenJDK" - k4eGhwn+KLmEkznRwWxO2klembT3P16HMrIc6RsnFxOthiuAgFDU8SHYL31zP7nVOGko6mF1zJI= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
      |183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:53:53 +0000] 109.157.193.10 arn:aws:iam::152813717728:user/stevel-dev 0966E574330716D9 REST.PUT.OBJECT fork-0004/test/eventually-reopen.dat "PUT /fork-0004/test/eventually-reopen.dat HTTP/1.1" 200 - - 7 49 23 "https://hadoop.apache.org/audit/dbfdceb7-a11a-4c39-91a1-49f286bf5a36-14.53.47/s/0000000d/op/dbfdceb7-a11a-4c39-91a1-49f286bf5a36-14.53.47?principal=stevel&op=op_create&path=s3a://stevel-london/fork-0004/test/eventually-reopen.dat" "Hadoop 3.4.0-SNAPSHOT, aws-sdk-java/1.11.901 Mac_OS_X/10.15.7 OpenJDK_64-Bit_Server_VM/25.252-b09 java/1.8.0_252 vendor/AdoptOpenJDK" - FH0SAldavv4Q3TavjvSVnbdvJ0mxPcIn9M40wjYI3EHwE+v2dDaEowgHUj5Y2AEcFTmzz6LRx0w= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
      |183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:53:53 +0000] 109.157.193.10 arn:aws:iam::152813717728:user/stevel-dev D630F8F6B949DA00 BATCH.DELETE.OBJECT fork-0004/ - 204 - - - - - - - - WVzx2GM3UeoXx11TVb+v1u1reJl+ahOiJApP9NDIHWBQPF87OXQyKKc/31NGXPNXLSEigGJZleg= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
      |183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:53:53 +0000] 109.157.193.10 arn:aws:iam::152813717728:user/stevel-dev D630F8F6B949DA00 BATCH.DELETE.OBJECT fork-0004/test/ - 204 - - - - - - - - WVzx2GM3UeoXx11TVb+v1u1reJl+ahOiJApP9NDIHWBQPF87OXQyKKc/31NGXPNXLSEigGJZleg= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
      |183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:53:53 +0000] 109.157.193.10 arn:aws:iam::152813717728:user/stevel-dev D630F8F6B949DA00 REST.POST.MULTI_OBJECT_DELETE - "POST /?delete HTTP/1.1" 200 - 116 - 73 - "https://hadoop.apache.org/audit/dbfdceb7-a11a-4c39-91a1-49f286bf5a36-14.53.47/s/0000000d/op/dbfdceb7-a11a-4c39-91a1-49f286bf5a36-14.53.47?principal=stevel&op=op_create&path=s3a://stevel-london/fork-0004/test/eventually-reopen.dat" "Hadoop 3.4.0-SNAPSHOT, aws-sdk-java/1.11.901 Mac_OS_X/10.15.7 OpenJDK_64-Bit_Server_VM/25.252-b09 java/1.8.0_252 vendor/AdoptOpenJDK" - WVzx2GM3UeoXx11TVb+v1u1reJl+ahOiJApP9NDIHWBQPF87OXQyKKc/31NGXPNXLSEigGJZleg= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
      |183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:54:09 +0000] 109.157.193.10 arn:aws:iam::152813717728:user/stevel-dev 039450FC77B1F9B1 BATCH.DELETE.OBJECT fork-0001/test/ - 204 - - - - - - - - do8tYo9l/uQgU21o7i6s5vAWN9yjKUBqHtPGP9j0rCjnyfybLVPVcau5baOVyW8KvmswujgyG2o= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
      |183c9826b45486e485693808f38e2c4071004bf5dfd4c3ab210f0a21a4235ef8 stevel-london [24/Feb/2021:14:54:09 +0000] 109.157.193.10 arn:aws:iam::152813717728:user/stevel-dev 039450FC77B1F9B1 REST.POST.MULTI_OBJECT_DELETE - "POST /?delete HTTP/1.1" 200 - 116 - 91 - "https://hadoop.apache.org/audit/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.54.02/s/00000005/op/87c879d6-0515-4ee9-b223-4647dd6aea9a-14.54.02?principal=stevel&op=op_delete&path=s3a://stevel-london/fork-0001/test" "Hadoop 3.4.0-SNAPSHOT, aws-sdk-java/1.11.901 Mac_OS_X/10.15.7 OpenJDK_64-Bit_Server_VM/25.252-b09 java/1.8.0_252 vendor/AdoptOpenJDK" - do8tYo9l/uQgU21o7i6s5vAWN9yjKUBqHtPGP9j0rCjnyfybLVPVcau5baOVyW8KvmswujgyG2o= SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader stevel-london.s3.eu-west-2.amazonaws.com TLSv1.2
      |""".stripMargin.split('\n')

  /**
   * Tests a large number of objects to see if there's any clash in versions.
   * This is a safety check un case versioning is inadequate
   */
  test("logs parse") {
    records.foreach {

      r =>
        logInfo(s"parsing $r")

        val p = S3LogRecordParser.parse(r)
        logInfo(s"$p")
    }

  }
}
