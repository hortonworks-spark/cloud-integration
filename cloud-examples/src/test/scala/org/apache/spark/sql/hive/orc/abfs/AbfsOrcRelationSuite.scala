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

package org.apache.spark.sql.hive.orc.abfs

import com.cloudera.spark.cloud.abfs.AbfsTestSetup

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.CloudRelationBasicSuite

class AbfsOrcRelationSuite extends CloudRelationBasicSuite with AbfsTestSetup {

  import testImplicits._

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
    }
  }

  override val dataSourceName: String = classOf[OrcFileFormat].getCanonicalName

  ctest("SPARK-12218",
    "'Not' is included in ORC filter pushdown", false) {

    withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPathDir("SPARK-12218") { dir =>
        val path = s"${dir.toString}/table1"
        (1 to 5).map(i => (i, (i % 2).toString)).toDF("a", "b").write.orc(path)

        checkAnswer(
          spark.read.orc(path).where("not (a = 2) or not(b in ('1'))"),
          (1 to 5).map(i => Row(i, (i % 2).toString)))

        checkAnswer(
          spark.read.orc(path).where("not (a = 2 and b in ('1'))"),
          (1 to 5).map(i => Row(i, (i % 2).toString)))
      }
    }
  }


}
