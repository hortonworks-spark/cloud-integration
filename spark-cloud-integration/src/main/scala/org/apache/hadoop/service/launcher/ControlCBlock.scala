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

package org.apache.hadoop.service.launcher

import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.util.ExitUtil

import org.apache.spark.internal.Logging

class ControlCBlock(limit: Long) extends IrqHandler.Interrupted with Logging {
  val counter = new AtomicLong(0)
  @Override def interrupted(interruptData: IrqHandler.InterruptData): Unit = {
    val c = counter.addAndGet(1)
    if (c < limit) {
      logWarning(s"Ignoring interrupt #$c")
    } else {
      logError("Reached interrupt limit; existing")
      ExitUtil.terminate(1, "Interrupted")
    }

  }
}


object ControlCBlock {

  def bind(): IrqHandler = {
    val controlC = new IrqHandler("INT", new ControlCBlock(5))
    controlC.bind()
    controlC
  }
}

