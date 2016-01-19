package com.github.spirom.sparkflights.util

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class PairAggregateCombiner[K:ClassTag, V: ClassTag, A <: Accumulator[V] : ClassTag]
  extends Serializable
{

  def initial: A

  private def add(acc: A, value: V): A = {
    acc.increment(value)
    acc
  }

  private def combine(acc1: A, acc2: A): A = {
    val acc = initial
    acc.merge(acc1)
    acc.merge(acc2)
    acc
  }

  def aggregateByKey(pairs: RDD[(K, V)]): RDD[(K, A)] =
  {
    pairs.aggregateByKey(initial)(add, combine)
  }
}