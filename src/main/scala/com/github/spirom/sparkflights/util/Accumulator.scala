package com.github.spirom.sparkflights.util

import scala.reflect.ClassTag


abstract class Accumulator[V: ClassTag] extends Serializable
{
  def increment(value: V): Unit

  def merge(other: Accumulator[V]): Unit
}
