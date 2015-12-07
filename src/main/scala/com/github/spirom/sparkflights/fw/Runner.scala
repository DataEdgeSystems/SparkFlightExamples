package com.github.spirom.sparkflights.fw

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.Logger
import org.apache.spark.SparkContext

class Runner(experiments: Iterable[Experiment]) {

  val logger = Logger.getLogger(getClass.getName)

  val results = new Results()

  val runId = makeIdFromDateTime()

  def makeIdFromDateTime(): String = {
    val now = Calendar.getInstance().getTime()
    val idFormat = new SimpleDateFormat("YYYY_MM_DD_hhmmss")
    val id = idFormat.format(now)
    id
  }

  def run(outputBase: String) : Unit = {

    val runOutputBase = outputBase + "/" + runId

    logger.info(s"Running all experiments for run $runId")

    logger.info(s"Saving results under $runOutputBase")

    experiments.foreach(e => e.run(runOutputBase, results))

    logger.info(s"Completed all experiments for run $runId")

  }

  def saveSummary(outputBase: String, sc: SparkContext) : Unit = {
    val runOutputBase = outputBase + "/" + runId

    results.save(runOutputBase + "/summary", sc)
  }
}
