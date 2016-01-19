package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.experiments.common.ByYearAdderCombiner
import com.github.spirom.sparkflights.fw.CoreExperiment
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class TopAirlinesByAnnualDeparturesCore(sc: SparkContext)
  extends CoreExperiment("TopAirlinesByAnnualDeparturesCore", sc) {

  def runUserCode(sc: SparkContext, df: DataFrame, outputBase: String): Unit = {
    val departures: RDD[(String, Int)] =
      df.select("uniquecarrier", "year").map(r =>
      (r.getString(0), r.getInt(1)))

    val comb = new ByYearAdderCombiner[String]
    val byCarrierKey =
      comb.aggregateByKey(departures)
    val carriersWithAverage = byCarrierKey.map(
      { case (year, acc) => (year, acc.average()) }
    )
    val sortedByAverage =
      carriersWithAverage.sortBy( { case (_, ave) => ave }, ascending=false)
    val top50 = sc.parallelize(sortedByAverage.take(50), 1)

    top50.saveAsTextFile(s"$outputBase/top_carriers_ave_departures")

    val totalCount = sortedByAverage.count()
    sc.parallelize(Seq(totalCount), 1).saveAsTextFile(s"$outputBase/carrier_count")

  }

}
