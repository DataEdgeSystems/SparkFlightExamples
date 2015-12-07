package com.github.spirom.sparkflights.fw

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.Logger

class Runner(experiments: Iterable[Experiment]) {

  val logger = Logger.getLogger(getClass.getName)

  val results = new Results()

  def makeIdFromDateTime(): String = {
    val now = Calendar.getInstance().getTime()
    val idFormat = new SimpleDateFormat("YYYY_MM_DD_hhmmss")
    val id = idFormat.format(now)
    id
  }

  def run(outputBase: String) : Unit = {

    val runId = makeIdFromDateTime()

    val runOutputBase = outputBase + "/" + runId

    logger.info("Running all experiments for run $runId")

    logger.info(s"Saving results under $runOutputBase")

    experiments.foreach(e => e.run(runOutputBase, results))

    logger.info("Completed all experiments for run $runId")

  }
}
