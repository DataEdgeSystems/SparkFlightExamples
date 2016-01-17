package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.CoreExperiment
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class TailNumbersCore(sc: SparkContext)
  extends CoreExperiment("TailNumbersCore", sc) {

  //
  // What aircraft tail numbers are covered in the data and
  // how many scheduled flights per tail number.
  //

  def runUserCode(sc: SparkContext, df: DataFrame, outputBase: String): Unit = {
    // don't need anything more than the years
    val tails: RDD[String] = df.select("tailnum").map(r =>
      r.getString(0))
    // count instances of each year
    val tailsWithCount = tails.groupBy(r => r).map(r => (r._1, r._2.size))

    tailsWithCount.saveAsTextFile(s"$outputBase/tails_with_counts")

  }

}
