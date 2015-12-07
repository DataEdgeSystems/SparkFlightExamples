package com.github.spirom.sparkflights.fw

import scala.collection.mutable

class Results {
  val results = new mutable.ListBuffer[ExperimentResult]()

  def add(result: ExperimentResult) : Unit = {
    results += result
  }

  def all = results.iterator
}
