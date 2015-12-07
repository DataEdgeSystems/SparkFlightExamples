package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.SQLExperiment
import org.apache.spark.sql.SQLContext

class YearsCoveredSQL(sqlContext: SQLContext)
  extends SQLExperiment("YearsCoveredSQL", sqlContext) {

  def runUserCode(sqlContext: SQLContext, outputBase: String): Unit = {
    val years = sqlContext.sql("SELECT DISTINCT year FROM flights ORDER BY year")
    years.rdd.saveAsTextFile(s"$outputBase/all_years_text")
  }

}
