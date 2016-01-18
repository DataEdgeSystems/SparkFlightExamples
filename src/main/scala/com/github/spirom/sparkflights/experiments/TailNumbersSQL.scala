package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.SQLExperiment
import org.apache.spark.sql.SQLContext

class TailNumbersSQL(sqlContext: SQLContext)
  extends SQLExperiment("TailNumbersSQL", sqlContext) {

  def runUserCode(sqlContext: SQLContext, outputBase: String): Unit = {

    // How frequently do the various tail numbers appear?

    val years = sqlContext.sql(
      s"""
        | SELECT tailnum, count(*) AS c
        | FROM flights
        | GROUP BY tailnum
        | ORDER BY c DESC
       """.stripMargin)
    years.rdd.saveAsTextFile(s"$outputBase/tails_with_counts")
  }

}
