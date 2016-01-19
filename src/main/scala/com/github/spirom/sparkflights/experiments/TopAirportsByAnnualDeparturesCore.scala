package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.experiments.common.ByYearAdderCombiner
import com.github.spirom.sparkflights.fw.CoreExperiment
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class TopAirportsByAnnualDeparturesCore(sc: SparkContext)
  extends CoreExperiment("TopAirportsByAnnualDeparturesCore", sc) {

  def runUserCode(sc: SparkContext, df: DataFrame, outputBase: String): Unit = {
    val departures: RDD[(String, Int)] =
      df.select("origin", "year").map(r =>
      (r.getString(0), r.getInt(1)))

    val comb = new ByYearAdderCombiner[String]
    val byOriginKey =
      comb.aggregateByKey(departures)

    val originsWithAverage = byOriginKey.map(
      { case (year, acc) => (year, acc.average()) }
    )
    val sortedByAverage =
      originsWithAverage.sortBy( { case (_, ave) => ave }, ascending=false)
    val top50 = sc.parallelize(sortedByAverage.take(50), 1)

    top50.saveAsTextFile(s"$outputBase/top_origins_ave_departures")

    val totalCount = sortedByAverage.count()
    sc.parallelize(Seq(totalCount), 1).saveAsTextFile(s"$outputBase/origin_count")

  }

}
