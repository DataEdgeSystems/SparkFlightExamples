package com.github.spirom.sparkflights.util

import scala.reflect.ClassTag

//
// The most general form of accumulator for aggregation.
//
abstract class Accumulator[D: ClassTag, ConcreteAcc <: Accumulator[D, _]]
  extends Serializable
{

  //type ConcreteAcc <: Accumulator[D]

  //
  // Include the given datum
  //
  def include(value: D): ConcreteAcc

  //
  // merge another accumulator into this one
  //
  def merge(other: ConcreteAcc): ConcreteAcc
}
