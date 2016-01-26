package com.github.spirom.sparkflights.util

import scala.reflect.ClassTag

//
// A wrapper that makes an accumulator "safe" by
// "hiding" any side-effecting behavior. Both the operations return a new, valid
// wrapped accumulator but invalidate the existing one -- if Spark attempts
// to use an accumulator on which either operation has been called, the
// resulting access will fail. This allows us to build efficient, complex
// accumulators and detect any situation where Spark violates the rather
// strong assumptions needed in order for their behavior to be correct.
//
class SafeAccumulatorWrapper[D: ClassTag, IA <: Accumulator[D, IA]](inner: IA)
  extends Accumulator[D, SafeAccumulatorWrapper[D, IA]] {

  var wrapped: Option[IA] = Some(inner)

  override def include(value: D): SafeAccumulatorWrapper[D, IA] = {
    wrapped match {
      case Some(a) => {
        val na = a.include(value)
        val next = new SafeAccumulatorWrapper[D, IA](na)
        wrapped = None
        next
      }
      case None => throw new IllegalStateException()
    }
  }

  override def merge(other: SafeAccumulatorWrapper[D, IA]): SafeAccumulatorWrapper[D, IA] = {
    wrapped match {
      case Some(a) => {
        other.wrapped match {
          case Some(o) => {
            val na = a.merge(o)
            val next = new SafeAccumulatorWrapper[D, IA](na)
            wrapped = None
            next
          }
          case None => throw new IllegalStateException()
        }
      }
      case None => throw new IllegalStateException()
    }
  }

}
