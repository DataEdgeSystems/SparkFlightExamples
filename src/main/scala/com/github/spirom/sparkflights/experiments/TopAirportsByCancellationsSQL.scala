package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.SQLExperiment
import org.apache.spark.sql.SQLContext


class TopAirportsByCancellationsSQL(sqlContext: SQLContext)
  extends SQLExperiment("TopAirportsByCancellationsSQL", sqlContext) {

  def runUserCode(sqlContext: SQLContext, outputBase: String): Unit = {

    //
    // The 10 origin airports with the highest absolute number
    // of departure cancellations since 2000
    //

    val popularFlights = sqlContext.sql(
      s"""
        | SELECT origin, count(cancelled) AS total_cancellations
        | FROM flights
        | WHERE cancelled = '1' AND year >= '2000'
        | GROUP BY origin
        | ORDER BY total_cancellations DESC
        | LIMIT 10
       """.stripMargin)
    popularFlights.rdd.saveAsTextFile(s"$outputBase/top_cancellations")
  }

}
