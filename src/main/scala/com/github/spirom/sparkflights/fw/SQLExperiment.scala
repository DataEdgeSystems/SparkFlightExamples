package com.github.spirom.sparkflights.fw

import org.apache.spark.sql.{DataFrame, SQLContext}

abstract class SQLExperiment(name: String, sqlContext: SQLContext) extends Experiment(name) {

  // context may have been modified from the one in the constructor
  def runUserCode(sqlContext: SQLContext, outputBase: String): Unit

  def runQuery(df: DataFrame, runOutputBase: String): Unit = {
    runUserCode(sqlContext, runOutputBase + "/" + name)
  }

}
