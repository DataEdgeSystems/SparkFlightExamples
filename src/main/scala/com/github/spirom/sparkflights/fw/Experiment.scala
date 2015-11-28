package com.github.spirom.sparkflights.fw

import org.apache.log4j.Logger

class Experiment(name: String) {

  val logger = Logger.getLogger(getClass.getName)

  def run(): Unit = {
    logger.info("Running $name")
    logger.info("Completed $name")
  }
}
