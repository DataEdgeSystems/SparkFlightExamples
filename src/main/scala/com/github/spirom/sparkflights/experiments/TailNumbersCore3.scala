package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.CoreExperiment
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class TailNumbersCore3(sc: SparkContext)
  extends CoreExperiment("TailNumbersCore3", sc) {

  //
  // What aircraft tail numbers are covered in the data and
  // how many scheduled flights per tail number.
  //

  def runUserCode(sc: SparkContext, df: DataFrame, outputBase: String): Unit = {
    // don't need anything more than the years
    val tails: RDD[(String, String)] =
      df.select("tailnum", "origin").map(r => (r.getString(0), r.getString(1)))
    // count instances of each year
    val tailsWithCount =
      tails.map(
        {
          case (tail, origin) => (tail, 1)
        }
      ).reduceByKey(
        (acc1: Int, acc2: Int) => acc1 + acc2
      )

    val sortedByCount =
      tailsWithCount.sortBy( { case (_, count) => count }, ascending=false)

    sortedByCount.saveAsTextFile(s"$outputBase/tails_with_counts")

    val totalCount = tailsWithCount.count()
    sc.parallelize(Seq(totalCount), 1).saveAsTextFile(s"$outputBase/tail_count")

  }

}
