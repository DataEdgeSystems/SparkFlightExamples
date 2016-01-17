package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.experiments.common.{ByTailNumberAdderCombiner, ByYearAdderCombiner}
import com.github.spirom.sparkflights.fw.CoreExperiment
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class AirlinesByTotalTailNumbersCore(sc: SparkContext)
  extends CoreExperiment("AirlinesByTotalTailNumbersCore", sc) {

  def runUserCode(sc: SparkContext, df: DataFrame, outputBase: String): Unit = {
    val departures: RDD[(String, String)] =
      df.select("uniquecarrier", "tailnum").map(r =>
      (r.getString(0), r.getString(1)))

    val byCarrierKey =
      departures.aggregateByKey(ByTailNumberAdderCombiner.initial)(
        ByTailNumberAdderCombiner.add,
        ByTailNumberAdderCombiner.combine)
    val carriersWithAverage = byCarrierKey.map(
      { case (carrier, acc) => (carrier, acc.count()) }
    )
    val sortedByAverage =
      carriersWithAverage.sortBy( { case (_, ave) => ave }, ascending=false)
    val top50 = sc.parallelize(sortedByAverage.take(50), 1)

    top50.saveAsTextFile(s"$outputBase/carriers_by_tail_number_count")

  }

}
