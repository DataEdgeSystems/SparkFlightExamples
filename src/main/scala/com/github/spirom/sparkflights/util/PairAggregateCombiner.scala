package com.github.spirom.sparkflights.util

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

//
// Encapsulates PairRDDFunctions.aggregateByKey and its 'seqOp' and 'combOp'
// based on the concept of an Accumulator abstraction. The 'seqOp' is implemented
// by adding a datum to the accumulator and the combOp is implemented by creating a
// new accumulator and merging int he old ones.
//
// The Accumulator implementation is provided when extending this class.
//
abstract class PairAggregateCombiner[K:ClassTag, V: ClassTag, A <: Accumulator[V] : ClassTag]
  extends Serializable
{

  def initial: A

  private def seq(acc: A, value: V): A = {
    acc.include(value)
    acc
  }

  private def comb(acc1: A, acc2: A): A = {
    val acc = initial
    acc.merge(acc1)
    acc.merge(acc2)
    acc
  }

  def aggregateByKey(pairs: RDD[(K, V)]): RDD[(K, A)] =
  {
    pairs.aggregateByKey(initial)(seq, comb)
  }
}