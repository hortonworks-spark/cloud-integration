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

package com.hortonworks.spark.cloud.s3

import java.io.{FileNotFoundException, IOException}
import java.util
import java.util.{ArrayList, List}

import com.hortonworks.spark.cloud.ObjectStoreOperations
import com.hortonworks.spark.cloud.persist.SuccessData
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.scalatest.Assertions

/**
 * General S3A operations against a filesystem.
 */
class S3AOperations(fs: FileSystem) extends ObjectStoreOperations with
  Assertions {

  /**
   * S3A Filesystem.
   */
   private val s3aFS = fs.asInstanceOf[S3AFileSystem]

  /**
   * Verify that an S3A committer was used
   * @param destDir destination directory of work
   * @param committer commiter name, if known
   */
   def verifyS3Committer(destDir: Path, committer: Option[String]): Unit = {
     s3aFS.getFileStatus(destDir)
     val successFile = new Path(destDir, SUCCESS_FILE_NAME)

     try {
       val status = s3aFS.getFileStatus(successFile)
       assert(status.getLen != 0, "Not committed with an S3A committer :" + destDir)
       val successData = SuccessData.load(s3aFS, successFile)
       logInfo(s"success data at $successFile : ${successData.toString}")
       logInfo(successData.dumpMetrics("  ", " =  ", "\n"))

       committer.foreach(n =>
         assert(n === successData.getCommitter, s"in $successData"))
       val files = successData.getFilenames
       assert(files != null, s"No 'filenames' in $successData")
       // TODO: file analysis

     } catch {
       case _: FileNotFoundException =>
         throw new FileNotFoundException("No commit success file: " + successFile)
     }
   }

}
