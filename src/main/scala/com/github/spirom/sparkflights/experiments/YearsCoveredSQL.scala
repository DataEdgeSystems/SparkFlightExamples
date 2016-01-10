package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.SQLExperiment
import org.apache.spark.sql.SQLContext

class YearsCoveredSQL(sqlContext: SQLContext)
  extends SQLExperiment("YearsCoveredSQL", sqlContext) {

  def runUserCode(sqlContext: SQLContext, outputBase: String): Unit = {

    // what years are covered in the data?

    val years = sqlContext.sql(
      s"""
        | SELECT DISTINCT year
        | FROM flights
        | ORDER BY year
       """.stripMargin)
    years.rdd.saveAsTextFile(s"$outputBase/all_years_text")
  }

}
