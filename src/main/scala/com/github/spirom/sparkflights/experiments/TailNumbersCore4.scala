package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.CoreExperiment
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.col

class TailNumbersCore4(sc: SparkContext)
  extends CoreExperiment("TailNumbersCore4", sc) {

  //
  // What aircraft tail numbers are covered in the data and
  // how many scheduled flights per tail number.
  //

  def runUserCode(sc: SparkContext, df: DataFrame, outputBase: String): Unit = {
    // don't need anything more than the years
    val tailsWithOne: RDD[(String, Int)] =
      df.select(col("tailnum"), lit(1).as("count")).map(r => (r.getString(0), r.getInt(1)))
    // count instances of each year
    val tailsWithCount =
      tailsWithOne.reduceByKey(
        (acc1: Int, acc2: Int) => acc1 + acc2
      )

    val sortedByCount =
      tailsWithCount.sortBy( { case (_, count) => count }, ascending=false)

    sortedByCount.saveAsTextFile(s"$outputBase/tails_with_counts")

    val totalCount = tailsWithCount.count()
    sc.parallelize(Seq(totalCount), 1).saveAsTextFile(s"$outputBase/tail_count")

  }

}
