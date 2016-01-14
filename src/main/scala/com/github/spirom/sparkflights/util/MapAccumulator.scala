package com.github.spirom.sparkflights.util

import scala.collection.mutable

abstract class MapAccumulator[K, V]() extends Serializable{

  val entries = new mutable.HashMap[K, V]()

  def mergeEntry(key: K, value: V): Unit = {
    entries.get(key) match {
      case Some(oldValue) => {
        val newValue = mergeValues(value, oldValue)
        entries.+=((key, newValue))
      }
      case None => entries.+=((key, value))
    }
  }

  def mergeValues(e1: V, e2: V) : V

  def merge(other: MapAccumulator[K, V]): Unit = {
    for ((k, v) <- other.entries.iterator) {
      mergeEntry(k, v)
    }
  }


}
