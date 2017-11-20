/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.cloud.s3;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)

public class TestInstantiateS3AClasses {

  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"org.apache.hadoop.fs.s3a.S3AFileSystem"},
        {"com.amazonaws.services.s3.S3ClientOptions"},
        {"org.apache.hadoop.fs.s3a.S3ABlockOutputStream"},
        {"org.apache.hadoop.fs.s3a.commit.magic.MagicCommitTracker"},
        {"org.apache.hadoop.fs.s3a.commit.MagicCommitPaths"},
        {"org.apache.hadoop.fs.s3a.commit.CommitUtils"},
        {"org.apache.hadoop.fs.s3a.commit.files.PendingSet"},
        {"org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit"},
        {"org.apache.hadoop.fs.s3a.commit.files.SuccessData"},
        {"org.apache.hadoop.fs.s3a.commit.CommitConstants"},
        {"org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore"},
        {"org.apache.hadoop.fs.s3a.S3ARetryPolicy"},
        {""},
        {""}
    });
  }

  private final String classname;

  public TestInstantiateS3AClasses(String classname) {
    this.classname = classname;
  }

  @Test
  public void testInstantiated() throws Throwable {
    if (classname.isEmpty()) {
      return;
    }
    try {
      this.getClass().getClassLoader().loadClass(classname);
    } catch (Exception e) {
      throw e;
    } catch (Throwable e) {
      throw new Exception("Could not instantiate " + classname
          + ": " + e, e);
    }

  }
}
