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

package org.apache.spark.cloudera

import scala.collection.immutable.NumericRange
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd.{ParallelCollectionPartition, RDD}

/**
 * Based on ParallelCollectionRDD; allows you to provide a function
 * mapping partition index to locations.
 */

class ParallelizedWithLocalityRDD[T: ClassTag](
    sc: SparkContext,
    @transient private val data: Seq[T],
    numSlices: Int,
    locationPrefs: (Int) => Seq[String])
  extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val slices = ParallelizedWithLocalityRDD.slice(data, numSlices).toArray
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i)))
      .toArray
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    new InterruptibleIterator(context,
      s.asInstanceOf[ParallelCollectionPartition[T]].iterator)
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    locationPrefs(s.index)
  }
}

object ParallelizedWithLocalityRDD {

  /**
   * Slice a collection into numSlices sub-collections. One extra thing we do here is to treat Range
   * collections specially, encoding the slices as other Ranges to minimize memory cost. This makes
   * it efficient to run Spark over RDDs representing large sets of numbers. And if the collection
   * is an inclusive Range, we use inclusive range for the last slice.
   */
  def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException(
        "Positive number of partitions required")
    }

    // Sequences need to be sliced at the same set of index positions for operations
    // like RDD.zip() to behave as expected
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }

    seq match {
      case r: Range =>
        positions(r.length, numSlices).zipWithIndex
          .map { case ((start, end), index) =>
            // If the range is inclusive, use inclusive range for the last slice
            if (r.isInclusive && index == numSlices - 1) {
              new Range.Inclusive(r.start + start * r.step, r.end, r.step)
            }
            else {
              new Range.Inclusive(r.start + start * r.step,
                r.start + end * r.step,
                r.step)
            }
          }.toSeq.asInstanceOf[Seq[Seq[T]]]
      case nr: NumericRange[T] =>
        // For ranges of Long, Double, BigInteger, etc
        val slices = new ArrayBuffer[Seq[T]](numSlices)
        var r = nr
        for ((start, end) <- positions(nr.length, numSlices)) {
          val sliceSize = end - start
          slices += r.take(sliceSize).asInstanceOf[Seq[T]]
          r = r.drop(sliceSize)
        }
        slices.toSeq
      case _ =>
        val array = seq.toArray // To prevent O(n^2) operations for List etc
        positions(array.length, numSlices).map { case (start, end) =>
          array.slice(start, end).toSeq
        }.toSeq
    }
  }
}
