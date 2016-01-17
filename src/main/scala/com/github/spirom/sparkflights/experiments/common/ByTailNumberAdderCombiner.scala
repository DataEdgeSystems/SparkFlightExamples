package com.github.spirom.sparkflights.experiments.common

import com.github.spirom.sparkflights.util.MapAccumulator


object ByTailNumberAdderCombiner {

  class ByTailNumberAccumulator() extends MapAccumulator[String, Int] {

    def increment(tailNum: String): Unit = {
      add(tailNum, 1)
    }

    def add(tailNum: String, count: Int): Unit = {
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

  def initial: ByTailNumberAccumulator = new ByTailNumberAccumulator()

  def add(acc: ByTailNumberAccumulator, tailNum: String): ByTailNumberAccumulator = {
    acc.increment(tailNum)
    acc
  }

  def combine(acc1: ByTailNumberAccumulator, acc2: ByTailNumberAccumulator): ByTailNumberAccumulator = {
    val acc = new ByTailNumberAccumulator()
    acc.merge(acc1)
    acc.merge(acc2)
    acc
  }

}
