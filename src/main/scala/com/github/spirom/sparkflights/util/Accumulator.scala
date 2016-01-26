package com.github.spirom.sparkflights.util

import scala.reflect.ClassTag

//
// The most general form of accumulator for aggregation. It is up to the
// implementation to determine whether the two operations defined here have
// side effects ont he concrete instance.
//
abstract class Accumulator[D: ClassTag, ConcreteAcc <: Accumulator[D, _]]
  extends Serializable
{

  //
  // Create an Accumulator from this one that includes the given datum,
  // and return it.
  //
  def include(value: D): ConcreteAcc

  //
  // Create an accumulator from this one and the given one
  // that results from merging their values, and return it. What it means
  // to merge the values is defined by the derived class.
  //
  def merge(other: ConcreteAcc): ConcreteAcc
}
