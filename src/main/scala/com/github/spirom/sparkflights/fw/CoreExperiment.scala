package com.github.spirom.sparkflights.fw

import org.apache.spark.SparkContext

abstract class CoreExperiment(name: String, sc: SparkContext) extends Experiment(name) {

  // context may have been modified from the one in the constructor
  def runUserCode(sc: SparkContext, outputBase: String): Unit

  def runQuery(runOutputBase: String): Unit = {
    runUserCode(sc, runOutputBase + "/" + name)
  }

}
