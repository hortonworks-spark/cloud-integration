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

import java.util.regex.Matcher

import org.apache.hadoop.fs.s3a.audit.S3LogParser
import org.apache.hadoop.fs.s3a.audit.S3LogParser._


/**
 * Log parsing using s3a audit classes.
 */
object LogParser {

  private val pattern = S3LogParser.LOG_ENTRY_PATTERN

  private def entry(matcher: Matcher, group: String): String = {
    val g = matcher.group(group)
    assert(g != null, s"Group $group is null")
    assert(!g.isEmpty, s"Group $group is empty")
    g
  }

  private def longEntry(m: Matcher, group: String): Long = {
    entry(m, group).toLong
  }

  /**
   * Parse a line.
   * @param line line
   * @return the entry or None if the regexp didn't match
   * @throws AssertionError if a group is null/empty
   */
  def parse(line: String): Option[ServerLogEntry] = {
    val m = pattern.matcher(line)

    if (m.matches()) {
      return None
    } else {
      Some(ServerLogEntry(
        bucketowner = entry(m, OWNER_GROUP),
        bucket_name = entry(m, BUCKET_GROUP),
        requestdatetime = entry(m, TIMESTAMP_GROUP),
        remoteip = entry(m, REMOTEIP_GROUP),
        requester = entry(m, REQUESTER_GROUP),
        requestid = entry(m, REQUESTID_GROUP),
        operation = entry(m, VERB_GROUP),
        key = entry(m, KEY_GROUP),
        request_uri = entry(m, REQUESTURI_GROUP),
        httpstatus = entry(m, HTTP_GROUP),
        errorcode = entry(m, AWSERRORCODE_GROUP),
        bytessent = longEntry(m, BYTESSENT_GROUP),
        objectsize = longEntry(m, OBJECTSIZE_GROUP),
        totaltime = entry(m, TOTALTIME_GROUP),
        turnaroundtime = entry(m, TURNAROUNDTIME_GROUP),
        referrer = entry(m, REFERRER_GROUP),
        useragent = entry(m, USERAGENT_GROUP),
        versionid = entry(m, VERSION_GROUP),
        hostid = entry(m, HOSTID_GROUP),
        sigv = entry(m, SIGV_GROUP),
        ciphersuite = entry(m, CYPHER_GROUP),
        authtype = entry(m, AUTH_GROUP),
        endpoint = entry(m, ENDPOINT_GROUP),
        tlsversion = entry(m, TLS_GROUP)))
    }

  }

}
