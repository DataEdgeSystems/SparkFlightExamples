package com.github.spirom.sparkflights.experiments.common

import com.github.spirom.sparkflights.util.{SafeAccumulatorWrapper, PairAggregateCombiner, MapAccumulator}

import scala.reflect.ClassTag


//
// Accumulate a map of tail numbers with counts for use in aggregation
//
class ByTailNumberAccumulator() extends MapAccumulator[String, Int, ByTailNumberAccumulator] {

  override def include(tailNum: String): ByTailNumberAccumulator = {
    add(tailNum, 1)
    this
  }

  private def add(tailNum: String, count: Int): Unit = {
    entries.get(tailNum) match {
      case Some(oldCount) => entries.+=((tailNum, oldCount + count))
      case None => entries.+=((tailNum, count))
    }
  }

  override def mergeValues(v1: Int, v2: Int): Int = v1 + v2

  def count(): Int = {
    entries.keySet.size
  }
}

//
// Aggregate tail numbers by some key
//
class ByTailNumberAdderCombiner[K:ClassTag]
  extends PairAggregateCombiner[K,String,ByTailNumberAccumulator]
{

  override def initial: ByTailNumberAccumulator = new ByTailNumberAccumulator()

}

//------------------------------------------------------------------

//
// Here we actually wrap our underlying accumulator
//
class ByTailNumberWrapper(acc: ByTailNumberAccumulator) extends
  SafeAccumulatorWrapper[String, ByTailNumberAccumulator](acc)
{

}
//
// SafeAccumulatorWrapper
//  D = String
//  IA = ByTailNumberAccumulator <: Accumulator[String, TailAccumulatorWWrapper]]
//extends Accumulator[D, SafeAccumulatorWrapper[D, IA]] {

/*
//
// Aggregate tail numbers by some key
//
class ByTailNumberAdderCombiner2[K:ClassTag]
  extends PairAggregateCombiner[K,String,ByTailNumberWrapper]
{

  override def initial: ByTailNumberWrapper =
    new ByTailNumberWrapper(new ByTailNumberAccumulator())

}
*/