package com.github.spirom.sparkflights.fw

import java.util.Date

class ExperimentResult(
                        val experiment: Experiment,
                        val start: Date,
                        val finish: Date,
                        val throwable: Option[Throwable]
                        ) {

}
