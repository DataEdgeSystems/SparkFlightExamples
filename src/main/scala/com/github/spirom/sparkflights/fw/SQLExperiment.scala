package com.github.spirom.sparkflights.fw

import org.apache.spark.sql.{DataFrame, SQLContext}

abstract class SQLExperiment(name: String, sqlContext: SQLContext) extends Experiment(name) {

  // context may have been modified from the one in the constructor
  def runUserCode(sqlContext: SQLContext, outputBase: String): Unit

  def runQuery(df: DataFrame, runOutputBase: String, index: Int): Unit = {
    val prefix = String.format("%05d", int2Integer(index))
    runUserCode(sqlContext, runOutputBase + "/" + prefix + "_" + name)
  }

}
