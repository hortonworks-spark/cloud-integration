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

package com.hortonworks.spark.cloud.s3.commit

import scala.collection.immutable

/**
 * Case class for the dataframes
 */
case class Event(
    year: Int, month: Int, day: Int, ymd: Int, monthname: String,
    datestr: String, value: String)

object Events {

  /**
   * Build up an event sequence across years, every month in every
   * year has "rows" events generated.
   * @param year1 start year
   * @param year2  end year
   * @param startMonth start month
   * @param endMonth end month
   * @param rows rows per month
   * @return the event sequence.
   */
  def events(
      year1: Int,
      year2: Int,
      startMonth: Int,
      endMonth: Int,
      rows: Int): immutable.IndexedSeq[Event] = {
    for (year <- year1 to year2;
      month <- startMonth to endMonth;
      day <- 1 to Months(month - 1)._2;
      r <- 1 to rows)
      yield event(year,
        month,
        day,
        "%d/%04f".format(r, Math.random() * 10000))
  }

  def monthCount(
      year1: Int,
      year2: Int,
      startMonth: Int,
      endMonth: Int): Int = {
    var count = 0
    for (year <- year1 to year2;
      month <- startMonth to endMonth)
      count += 1
    count
  }

  /**
   * Create an event
   *
   * @return the event.
   */
  def event(year: Int, month: Int, day: Int, value: String): Event = {
    new Event(year, month, day,
      day + month * 100 + year * 10000,
      Months(month - 1)._1,
      "%04d-%02d0-%02d".format(year, month, day),
      value
    )
  }

  val Months = Array(
    ("Jan", 31),
    ("Feb", 28),
    ("Mar", 31),
    ("Apr", 30),
    ("May", 31),
    ("Jun", 30),
    ("Jul", 31),
    ("Aug", 31),
    ("Sep", 30),
    ("Oct", 31),
    ("Nov", 30),
    ("Dec", 31))

}
