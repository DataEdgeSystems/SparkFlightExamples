package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.SQLExperiment
import org.apache.spark.sql.SQLContext


class TopAirportsByLongDelaysSQL(sqlContext: SQLContext)
  extends SQLExperiment("TopAirportsByLongDelaysSQL", sqlContext) {

  def runUserCode(sqlContext: SQLContext, outputBase: String): Unit = {

    //
    // The 10 airports with the highest absolute number of
    // scheduled flights experiencing a departure delay over 60 minutes
    // since 2000
    //
    val longDepDelay = sqlContext.sql(
      s"""
        | SELECT origin, count(depdelay) as cnt
        | FROM flights
        | WHERE depdelay >= '60' AND year >= '2000'
        | GROUP BY origin
        | ORDER BY cnt DESC
        | LIMIT 10
       """.stripMargin)
    longDepDelay.rdd.saveAsTextFile(s"$outputBase/top_long_delays")
  }

}
