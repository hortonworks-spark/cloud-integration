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

package com.cloudera.spark.cloud.s3.audit

case class ServerLogEntry(
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
