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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)

public class TestRejectMRClasses {

  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter"},
        {"org.apache.hadoop.fs.s3a.commit.staging.PartitionedStagingCommitterFactory"},
        {"org.apache.hadoop.mapreduce.lib.output.FileOutputFormat"},
        {"org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter"},
        {""},
    });
  }

  private final String classname;

  public TestRejectMRClasses(String classname) {
    this.classname = classname;
  }

  @Test
  public void testInstantiated() {
    if (classname.isEmpty()) {
      return;
    }
    try {
      Class<?> clazz = this.getClass().getClassLoader().loadClass(classname);
      Assert.fail("Loaded class " + classname + " as " + clazz );
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      // expected
    }
  }

}
