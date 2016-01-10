package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.SQLExperiment
import org.apache.spark.sql.SQLContext


class MostPopularRoutesSQL(sqlContext: SQLContext)
  extends SQLExperiment("MostPopularRoutesSQL", sqlContext) {

  def runUserCode(sqlContext: SQLContext, outputBase: String): Unit = {

    //
    // The 10 most popular origin/destination airport pairs
    // (by absolute number of scheduled flights) since 2000
    //

    val popularFlights = sqlContext.sql(
      s"""
        | SELECT origin, dest, count(*) AS total_flights
        | FROM flights
        | WHERE year >= '2000'
        | GROUP BY origin, dest
        | ORDER BY total_flights DESC
        | LIMIT 10
       """.stripMargin)
    popularFlights.rdd.saveAsTextFile(s"$outputBase/popular_flights")
  }

}
