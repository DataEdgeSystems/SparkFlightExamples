package com.github.spirom.sparkflights.util

import scala.collection.mutable
import scala.reflect.ClassTag

//
// An accumulator that uses the datum as a kay into a map
//
abstract class MapAccumulator[K: ClassTag, V: ClassTag] extends Accumulator[K] {

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

  override def merge(other: Accumulator[K]): Unit = {
    other match {
      case kvOther: MapAccumulator[K,V] => {
        for ((k, v) <- kvOther.entries.iterator) {
          mergeEntry(k, v)
        }
      }
      case _ => throw new ClassCastException
    }

  }


}
