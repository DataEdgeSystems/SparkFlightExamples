package com.github.spirom.sparkflights.util

import scala.collection.mutable
import scala.reflect.ClassTag

//
// An accumulator that uses the datum as a kay into a map
//
abstract class MapAccumulator[K: ClassTag, V: ClassTag,
  MA <: MapAccumulator[K, V, _]]
  extends Accumulator[K, MA]
{

  val entries = new mutable.HashMap[K, V]()

  private def mergeEntry(key: K, value: V): Unit = {
    entries.get(key) match {
      case Some(oldValue) => {
        val newValue = mergeValues(value, oldValue)
        entries.+=((key, newValue))
      }
      case None => entries.+=((key, value))
    }
  }

  def mergeValues(e1: V, e2: V) : V

  override def merge(other: MA): MA = {
    for ((k, v) <- other.entries.iterator) {
      mergeEntry(k, v)
    }
    this.asInstanceOf[MA]
  }

}
