package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.CoreExperiment
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object TopAirportsByLongDelaysPercent {

  case class Accumulator(delayed: Int, total: Int) {
    def percent = if (total > 0) 100.0 * delayed / total else 0.0
  }

  def add(acc: Accumulator, data:(Double, Int)): Accumulator = {
    val delayed = data match
    {
      case (delay, year) =>
        if (year > 2000 && delay > 60) acc.delayed + 1 else acc.delayed
    }
    val total = acc.total + 1
    Accumulator(delayed, total)
  }

  def combine(acc1: Accumulator, acc2: Accumulator): Accumulator = {
    Accumulator(acc1.delayed + acc2.delayed, acc1.total + acc2.total)
  }
}

class TopAirportsByLongDelaysPercentCore(sc: SparkContext)
  extends CoreExperiment("TopAirportsByLongDelaysPercentCore", sc) {

  def runUserCode(sc: SparkContext, df: DataFrame, outputBase: String): Unit = {
    val delays: RDD[(String, (Double, Int))] =
      df.select("origin", "depdelay", "year").map(r =>
      (r.getString(0), (r.getDouble(1), r.getInt(2))))
    val yearsWithCounts =
      delays.aggregateByKey(TopAirportsByLongDelaysPercent.Accumulator(0,0))(
        TopAirportsByLongDelaysPercent.add,
        TopAirportsByLongDelaysPercent.combine)
    val yearsWithPercent = yearsWithCounts.map(
      { case (year, acc) => (year, acc.percent) }
    )
    val sortedByPercent =
      yearsWithPercent.sortBy( { case (_, percent) => percent }, ascending=false)
    val top10 = sc.parallelize(sortedByPercent.take(10), 1)

    top10.saveAsTextFile(s"$outputBase/top_origins_long_delays_percent")

  }

}
