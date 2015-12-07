package com.github.spirom.sparkflights.fw

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

abstract class Experiment(val name: String) {

  val logger = Logger.getLogger(getClass.getName)

  def runQuery(df: DataFrame, runOutputBase: String): Unit

  def run(df: DataFrame, runOutputBase: String, results: Results): Unit = {

    val timeFormat = new SimpleDateFormat("hh:mm:ss")

    val before = Calendar.getInstance().getTime()
    logger.info(s"Running $name at ${timeFormat.format(before)}")

    try {
      runQuery(df, runOutputBase)

      val after = Calendar.getInstance().getTime()

      val diff = after.getTime - before.getTime

      val result = new ExperimentResult(this, before, after, None)
      results.add(result)

      logger.info(s"Completed $name at ${timeFormat.format(after)} after $diff seconds")
    } catch {
      case (t: Throwable) => {
        val after = Calendar.getInstance().getTime()

        val diff = after.getTime - before.getTime

        val result = new ExperimentResult(this, before, after, Some(t))
        results.add(result)

        logger.warn(s"Failed $name at ${timeFormat.format(after)} after $diff seconds", t)
      }
    }
  }
}
