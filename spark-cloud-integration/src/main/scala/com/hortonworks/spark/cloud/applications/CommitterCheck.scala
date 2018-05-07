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

package com.hortonworks.spark.cloud.applications

import java.net.URL

import com.hortonworks.spark.cloud.ObjectStoreExample
import com.hortonworks.spark.cloud.utils.IntegrationUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ExitUtil

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Performs a committer check.
 */
class CommitterCheck extends ObjectStoreExample {

  override protected def usageArgs(): String = {
    "[off|on|file|directory|staging|magic] <dest>"
  }

  private val E_BAD_CONFIG = 10
  private val integrationUtils = new IntegrationUtils


  def fail(s: String): Int = {
    println("Failure: " + s)
    E_BAD_CONFIG;
  }

  private val bindingParquet = "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"
  private val pathOutput = "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"

  private val parquetCommitterKey: String = SQLConf
    .PARQUET_OUTPUT_COMMITTER_CLASS.key
  private val sqlCommitterKey: String = SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key
  val required = Map(
    parquetCommitterKey -> bindingParquet,
    sqlCommitterKey -> pathOutput)


  def invoke(sc: SparkContext, args: Seq[String]): Int = {
    0

  }

  /**
   * Action to execute.
   *
   * @param sparkConf configuration to use
   * @param args      argument array
   * @return an exit code
   */
  override def action(
    sparkConf: SparkConf,
    args: Array[String]): Int = {

/*
    if (args.length != 2) {
      return usage()
    }
*/
/*

    val committerType = args(0)
    val destPath = new Path(args(1))
*/

    def logValue(key: String) = {
      val v = sparkConf.getOption(key)
      val result = v.getOrElse("*undefined*")
      println(s"$v = '$result'")
    }

    def validate(key: String, expected: String) = {
      val v = sparkConf.getOption(key).getOrElse {
        throw new ExitUtil.ExitException(E_BAD_CONFIG,
          s"Unset option $key: (expected '$expected')")
      }
      if (expected != v) {
        throw new ExitUtil.ExitException(E_BAD_CONFIG,
          s"Expected value of $key: '$expected', actual '$v'")
      }
    }

    sys.env
      .foreach(t => if (t._1.startsWith("SPARK")) println(s"${t._1} = ${t._2}"))


    // print
    required.foreach((t) => logValue(t._1))

    // now validate
    required.foreach((t) => validate(t._1, t._2))


    // try to instantiate the class
    println(s"Classpath =\n${System.getProperty("java.class.path")}")
    val committerName = sparkConf.get(sqlCommitterKey)
    val parquetCommitter = sparkConf.get(parquetCommitterKey)

    val classLoadMap = Map[String, String](
      sqlCommitterKey -> committerName,
      parquetCommitterKey -> parquetCommitter,
      "PathOutputCommitProtocol" ->
        "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
      "BindingParquetOutputC`ommitter" ->
        "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
      "StagingCommitter" ->
        "org.apache.hadoop.fs.s3a.commit.staging.StagingCommitter",
      "MagicS3GuardCommitter" ->
        "org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter"


    )

    classLoadMap.map { case (k, v) => integrationUtils.findClass(k, v) }
      .foreach { case (src: String, classname: String, url: URL, clazz: Class[_]) =>
        println(s"$classname from $src found at $url")
      }


    println("All classes found locally")
//    sparkConf.set("spark.default.parallelism", "4")

//    applyObjectStoreConfigurationOptions(sparkConf, false)
    //    hconf(sparkConf, S3AConstantsAndKeys.FAST_UPLOAD, "true")
    val spark = SparkSession
      .builder
      .appName("CommitterCheck")
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext


    val rdd: RDD[String]= sc.parallelize(classLoadMap.keySet.toSeq)

    rdd.foreach{ key =>
      val classname = classLoadMap(key)
      val r = new IntegrationUtils().findClass(key, classname)
      (r._1, r._2, r._3)
    }

    // done
    0

  }




}


object CommitterCheck {

  def main(args: Array[String]) {
    new CommitterCheck().run(args)
  }


}
