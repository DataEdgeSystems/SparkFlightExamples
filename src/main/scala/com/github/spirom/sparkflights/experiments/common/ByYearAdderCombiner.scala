package com.github.spirom.sparkflights.experiments.common
import com.github.spirom.sparkflights.util.MapAccumulator


object ByYearAdderCombiner {

  class ByYearAccumulator() extends MapAccumulator[Int, Int] {

    def increment(year: Int): Unit = {
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

  def initial: ByYearAccumulator = new ByYearAccumulator()

  def add(acc: ByYearAccumulator, year: Int): ByYearAccumulator = {
    acc.increment(year)
    acc
  }

  def combine(acc1: ByYearAccumulator, acc2: ByYearAccumulator): ByYearAccumulator = {
    val acc = new ByYearAccumulator()
    acc.merge(acc1)
    acc.merge(acc2)
    acc
  }

}
