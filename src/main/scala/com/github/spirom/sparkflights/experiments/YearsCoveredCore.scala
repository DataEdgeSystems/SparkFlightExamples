package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.CoreExperiment
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class YearsCoveredCore(sc: SparkContext) extends CoreExperiment("YearsCoveredCore", sc) {

  def runUserCode(sc: SparkContext, df: DataFrame, outputBase: String): Unit = {
    val years: RDD[Int] = df.select("year").map(r =>
      r.getInt(0))
    val yearsWithCount = years.groupBy(r => r).map(r => (r._1, r._2.size))

    yearsWithCount.saveAsTextFile(s"$outputBase/all_years_text")

  }

}
