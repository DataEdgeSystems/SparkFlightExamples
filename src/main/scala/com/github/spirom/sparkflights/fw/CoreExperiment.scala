package com.github.spirom.sparkflights.fw

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

abstract class CoreExperiment(name: String, sc: SparkContext) extends Experiment(name) {

  // context may have been modified from the one in the constructor
  def runUserCode(sc: SparkContext, df: DataFrame, outputBase: String): Unit

  def runQuery(df: DataFrame, runOutputBase: String): Unit = {
    runUserCode(sc, df, runOutputBase + "/" + name)
  }

}
