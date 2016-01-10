package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.CoreExperiment
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object TopAirportsByAnnualDepartures {

  case class Accumulator() {
    val byYear = new mutable.HashMap[Int, Int]()

    def increment(year: Int): Unit = {
      add(year, 1)
    }

    def add(year: Int, count: Int): Unit = {
      byYear.get(year) match {
        case Some(oldCount) => byYear.+=((year, oldCount + count))
        case None => byYear.+=((year, count))
      }
    }

    def merge(other: Accumulator): Unit = {
      for ((year, count) <- other.byYear.iterator) {
        add(year, count)
      }
    }

    def average(): Int = {
      val v = byYear.values.seq
      if (v.isEmpty)
        0
      else
        v.sum / v.size
    }
  }

  def add(acc: Accumulator, year: Int): Accumulator = {
    if (year >= 2000 && year <= 2014) {
      acc.increment(year)
      acc
    } else
      acc
  }

  def combine(acc1: Accumulator, acc2: Accumulator): Accumulator = {
    val acc = Accumulator()
    acc.merge(acc1)
    acc.merge(acc2)
    acc
  }


}

class TopAirportsByAnnualDeparturesCore(sc: SparkContext)
  extends CoreExperiment("TopAirportsByAnnualDeparturesCore", sc) {

  def runUserCode(sc: SparkContext, df: DataFrame, outputBase: String): Unit = {
    val departures: RDD[(String, Int)] =
      df.select("origin", "year").map(r =>
      (r.getString(0), r.getInt(1)))

    val byOriginKey =
      departures.aggregateByKey(TopAirportsByAnnualDepartures.Accumulator())(
        TopAirportsByAnnualDepartures.add,
        TopAirportsByAnnualDepartures.combine)
    val originsWithAverage = byOriginKey.map(
      { case (year, acc) => (year, acc.average()) }
    )
    val sortedByAverage =
      originsWithAverage.sortBy( { case (_, ave) => ave }, ascending=false)
    val top50 = sc.parallelize(sortedByAverage.take(50), 1)

    top50.saveAsTextFile(s"$outputBase/top_origins_ave_departures")

  }

}
