package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.CoreExperiment
import com.github.spirom.sparkflights.util.PairAggregateCounter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class AirportPairsCore(sc: SparkContext)
  extends CoreExperiment("AirportPairsCore", sc) {

  def runUserCode(sc: SparkContext, df: DataFrame, outputBase: String): Unit = {

    // first analyze origin/destination pairs by frequency

    val pairs: RDD[((String, String), Int)] =
      df.select("origin", "dest", "flightnum").map(r =>
      ((r.getString(0), r.getString(1)), r.getInt(2)))

    val comb = new PairAggregateCounter[(String, String)]()
    val countsByPair = comb.aggregateByKey(pairs)

    val sortedByCount =
      countsByPair.sortBy( { case (_, count) => count }, ascending=false)
    val top50 = sc.parallelize(sortedByCount.take(50), 1)

    top50.saveAsTextFile(s"$outputBase/top_counts_by_pair")

    val totalCount = countsByPair.count()
    sc.parallelize(Seq(totalCount), 1).saveAsTextFile(s"$outputBase/pair_count")

    // next analyze airports by departures

    val departures: RDD[(String, Int)] =
      df.select("origin", "flightnum").map(r =>
        (r.getString(0), r.getInt(1)))

    val depCounter = new PairAggregateCounter[String]
    val countByOrigin = depCounter.aggregateByKey(departures)

    // then join the count of departures onto the origin/destination

    val departureCountMap = countByOrigin.collectAsMap()

    val pairTripsAndDepartures = countsByPair.map({
      case ((origin, dest), flights) =>
        {
          val originDep = departureCountMap.get(origin) match {
            case Some(c) => c
            case None => 0
          }
          val destDep = departureCountMap.get(dest) match {
            case Some(c) => c
            case None => 0
          }
          ((origin, dest), flights, originDep, destDep)
        }
    })

    // finally, look for pairs where the number of flights is highest relative to
    // (a) the total number of flights out of either airport, and
    // (b) the inverse of the distance between the two airports (assuming that
    //     mostly two airports close to each other will have more flights

    val highForOrigin =
      pairTripsAndDepartures.sortBy( {
        case (_, flights, depCount, _) => flights/depCount }, ascending=false)
    val topForOrigin = sc.parallelize(highForOrigin.take(10), 1)
    topForOrigin.saveAsTextFile(s"$outputBase/top_for_origin")

    val highForDest =
      pairTripsAndDepartures.sortBy( {
        case (_, flights, _, depCount) => flights/depCount }, ascending=false)
    val topForDest = sc.parallelize(highForDest.take(10), 1)
    topForDest.saveAsTextFile(s"$outputBase/top_for_dest")

    // TODO: relative to distances

  }

}
