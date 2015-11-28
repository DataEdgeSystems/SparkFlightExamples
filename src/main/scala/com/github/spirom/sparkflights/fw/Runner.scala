package com.github.spirom.sparkflights.fw

import org.apache.log4j.Logger

class Runner(experiments: Seq[Experiment]) {

  val logger = Logger.getLogger(getClass.getName)

  def run() : Unit = {

    logger.info("Running all experiments")

    experiments.foreach(e => e.run())

    logger.info("Completed all experiments")

  }
}
