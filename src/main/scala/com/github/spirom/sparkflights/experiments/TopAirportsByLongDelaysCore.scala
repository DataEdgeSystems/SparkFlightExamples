package com.github.spirom.sparkflights.experiments

import com.github.spirom.sparkflights.fw.CoreExperiment
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object TopAirportsByLongDelays {
  def add(count: Int, data:(Double, Int)): Int = {
    data match
    {
      case (delay, year) => if (year > 2000 && delay > 60) count + 1 else count
    }
  }

  def combine(count1: Int, count2: Int): Int = {
    count1 + count2
  }
}

class TopAirportsByLongDelaysCore(sc: SparkContext)
  extends CoreExperiment("TopAirportsByLongDelaysCore", sc) {

  def runUserCode(sc: SparkContext, df: DataFrame, outputBase: String): Unit = {
    val delays: RDD[(String, (Double, Int))] =
      df.select("origin", "depdelay", "year").map(r =>
      (r.getString(0), (r.getDouble(1), r.getInt(2))))
    val originsWithCount =
      delays.aggregateByKey(0)(TopAirportsByLongDelays.add, TopAirportsByLongDelays.combine)
    val sortedByCount =
      originsWithCount.sortBy( { case (_, count) => count }, ascending=false)
    val top10 = sc.parallelize(sortedByCount.take(10), 1)

    top10.saveAsTextFile(s"$outputBase/top_origins_long_delays")

  }

}
