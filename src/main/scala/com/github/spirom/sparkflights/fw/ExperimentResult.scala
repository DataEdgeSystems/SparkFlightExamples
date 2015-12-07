package com.github.spirom.sparkflights.fw

import java.text.SimpleDateFormat
import java.util.Date

class ExperimentResult(
                        val experiment: Experiment,
                        val start: Date,
                        val finish: Date,
                        val throwable: Option[Throwable]
                        ) {

  private val timeFormat = new SimpleDateFormat("hh:mm:ss")

  val startString = timeFormat.format(start)

  val finishString = timeFormat.format(finish)

}
