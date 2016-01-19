package com.github.spirom.sparkflights.util

import scala.reflect.ClassTag

//
// The most general form of accumulator for aggregation.
//
abstract class Accumulator[D: ClassTag] extends Serializable
{
  //
  // Include the given datum
  //
  def include(value: D): Unit

  //
  // merge another accumulator into this one
  //
  def merge(other: Accumulator[D]): Unit
}
