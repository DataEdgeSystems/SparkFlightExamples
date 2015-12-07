package com.github.spirom.sparkflights.fw

import scala.collection.mutable


class Registry {

  private val experiments: mutable.HashMap[String, Experiment] =
    new mutable.HashMap[String, Experiment]()

  def add(experiment: Experiment) : Unit = {
    experiments += experiment.name -> experiment
  }

  def count():Int = {
    experiments.size
  }

  def lookup(name: String): Option[Experiment] = {
    experiments.get(name)
  }

  def getAll(): Iterable[Experiment] = {
    experiments.values
  }
}
