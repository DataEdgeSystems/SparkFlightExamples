package com.github.spirom.sparkflights.fw

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.Logger

abstract class Experiment(val name: String) {

  val logger = Logger.getLogger(getClass.getName)

  def runQuery(runOutputBase: String): Unit

  def run(runOutputBase: String, results: Results): Unit = {

    val timeFormat = new SimpleDateFormat("YYYY_MM_DD_hhmmss")

    val before = Calendar.getInstance().getTime()
    logger.info(s"Running $name at ${timeFormat.format(before)}")

    //TODO: try catch block and save throwable
    runQuery(runOutputBase)

    val after = Calendar.getInstance().getTime()

    val diff = after.getTime - before.getTime

    val result = new ExperimentResult(this, before, after, None)
    results.add(result)

    logger.info(s"Completed $name at ${timeFormat.format(after)} after $diff seconds")
  }
}
