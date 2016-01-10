package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.SQLExperiment
import org.apache.spark.sql.SQLContext


class TopAirportsByDeparturesSQL(sqlContext: SQLContext)
  extends SQLExperiment("TopAirportsByDeparturesSQL", sqlContext) {

  def runUserCode(sqlContext: SQLContext, outputBase: String): Unit = {

    //
    // The 10 airports with the highest absolute number of scheduled
    // departures since 2000
    //

    val topDepartures = sqlContext.sql(
      s"""
        | SELECT origin, count(*) AS total_departures
        | FROM flights
        | WHERE year >= '2000'
        | GROUP BY origin
        | ORDER BY total_departures DESC
        | LIMIT 10
       """.stripMargin)
    topDepartures.rdd.saveAsTextFile(s"$outputBase/top_departures")
  }

}
