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

package com.hortonworks.spark.cloud.common

import org.apache.spark.mllib.linalg.Vectors;

/**
 * A sample of a read operation.
 * @param started start time in nS
 * @param duration duration nS
 * @param blockSize size of block worked with
 * @param bytesRequested how many bytes were requested
 * @param bytesRead how many bytes were actually returned
 * @param pos position in the object where the read was requested.
 */
class ReadSample(
    val started: Long,
    val duration: Long,
    val blockSize: Int,
    val bytesRequested: Int,
    val bytesRead: Int,
    val pos: Long) extends Serializable {

  def perByte: Long = { if (duration > 0)  bytesRead / duration else -1L }

  def delta: Int = { bytesRequested - bytesRead }

  override def toString: String = s"ReadSample(started=$started, duration=$duration," +
      s" blockSize=$blockSize, bytesRequested=$bytesRequested, bytesRead=$bytesRead)" +
      s" pos=$pos"

  def toVector = {
    val a = new Array[Double](8)
    a(0) = started.toDouble
    a(1) = duration.toDouble
    a(2) = blockSize.toDouble
    a(3) = bytesRequested.toDouble
    a(4) = bytesRead.toDouble
    a(5) = pos.toDouble
    a(6) = perByte.toDouble
    a(7) = delta.toDouble
    Vectors.dense(a)
  }

}
