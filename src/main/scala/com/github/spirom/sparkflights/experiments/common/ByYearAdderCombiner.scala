package com.github.spirom.sparkflights.experiments.common
import com.github.spirom.sparkflights.util.{PairAggregateCombiner, MapAccumulator}

import scala.reflect.ClassTag

class ByYearAccumulator() extends MapAccumulator[Int, Int] {

  def include(year: Int): Unit = {
    add(year, 1)
  }

  def add(year: Int, count: Int): Unit = {
    entries.get(year) match {
      case Some(oldCount) => entries.+=((year, oldCount + count))
      case None => entries.+=((year, count))
    }
  }

  override def mergeValues(v1: Int, v2: Int): Int = v1 + v2

  def average(): Int = {
    val v = entries.values.seq
    if (v.isEmpty)
      0
    else
      v.sum / v.size
  }
}

class ByYearAdderCombiner[K:ClassTag]
  extends PairAggregateCombiner[K,Int,ByYearAccumulator] {

  override def initial: ByYearAccumulator = new ByYearAccumulator()

}
