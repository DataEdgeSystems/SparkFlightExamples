package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.SQLExperiment
import org.apache.spark.sql.SQLContext


class TopQuartersByCancellationsSQL(sqlContext: SQLContext)
  extends SQLExperiment("TopQuartersByCancellationsSQL", sqlContext) {

  def runUserCode(sqlContext: SQLContext, outputBase: String): Unit = {

    //Worst quarters for departure cancellations

    val quarterCancel = sqlContext.sql(
      s"""
        | SELECT quarter, count(cancelled) AS total_cancellations
        | FROM flights WHERE cancelled = '1'
        | GROUP BY quarter
        | ORDER BY total_cancellations DESC
        | LIMIT 10
       """.stripMargin)
    quarterCancel.rdd.saveAsTextFile(s"$outputBase/quarters_by_cancellations")
  }

}
