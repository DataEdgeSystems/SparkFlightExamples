package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.SQLExperiment
import org.apache.spark.sql.SQLContext


class TopAirportsByShortDelaysSQL(sqlContext: SQLContext)
  extends SQLExperiment("TopAirportsByShortDelaysSQL", sqlContext) {

  def runUserCode(sqlContext: SQLContext, outputBase: String): Unit = {

    //
    // The 10 airports with the highest absolute number of
    // scheduled flights experiencing a departure delay over 15 minutes
    // since 2000
    //

    val shortDepDelay = sqlContext.sql(
      s"""
        | SELECT origin, count(depdelay) as cnt
        | FROM flights
        | WHERE depdelay >= '15' AND year >= '2000'
        | GROUP BY origin
        | ORDER BY cnt DESC
        | LIMIT 10
       """.stripMargin)
    shortDepDelay.rdd.saveAsTextFile(s"$outputBase/top_short_delays")
  }

}
