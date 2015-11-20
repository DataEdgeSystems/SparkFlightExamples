
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.hive.HiveContext

object FlightSample {

  def runSqlQueries(outputLocation: String, hiveContext: HiveContext): Unit = {
    //Top 10 airports with the most departures since 2000
    val topDepartures = hiveContext.sql("SELECT origin, count(*) AS total_departures FROM flights WHERE year >= '2000' GROUP BY origin ORDER BY total_departures DESC LIMIT 10")
    topDepartures.rdd.saveAsTextFile(s"$outputLocation/top_departures")

    //Top 10 airports with the most departure delays over 15 minutes since 2000
    val shortDepDelay = hiveContext.sql("SELECT origin, count(depDelay) as cnt FROM flights WHERE depDelay >= '15' AND year >= '2000' GROUP BY origin ORDER BY cnt DESC LIMIT 10")
    shortDepDelay.rdd.saveAsTextFile(s"$outputLocation/top_short_delays")

    //Top 10 airports with the most departure delays over 60 minutes since 2000
    val longDepDelay = hiveContext.sql("SELECT origin, count(depDelay) AS total_delays FROM flights WHERE depDelay > '60' AND year >= '2000' GROUP BY origin ORDER BY total_delays DESC LIMIT 10")
    longDepDelay.rdd.saveAsTextFile(s"$outputLocation/top_long_delays")

    //Top 10 airports with the most departure cancellations since 2000
    val topCancel = hiveContext.sql("SELECT origin, count(cancelled) AS total_cancellations FROM flights WHERE cancelled = '1' AND year >= '2000' GROUP BY origin ORDER BY total_cancellations DESC LIMIT 10")
    topCancel.rdd.saveAsTextFile(s"$outputLocation/top_cancellations")

    //Rank of the worst quarter of the year for departure cancellations
    val quarterCancel = hiveContext.sql("SELECT quarter, count(cancelled) AS total_cancellations FROM flights WHERE cancelled = '1' GROUP BY quarter ORDER BY total_cancellations DESC LIMIT 10")
    quarterCancel.rdd.saveAsTextFile(s"$outputLocation/rank_quarter_cancellations")

    //Top 10 most popular flight routes since 2000
    val popularFlights = hiveContext.sql("SELECT origin, dest, count(*) AS total_flights FROM flights WHERE year >= '2000' GROUP BY origin, dest ORDER BY total_flights DESC LIMIT 10")
    popularFlights.rdd.saveAsTextFile(s"$outputLocation/popular_flights")
  }

  //
  // Configuration:
  //   local vs. cluster
  //   parquet vs. CSV
  //   which example to run
  //
  def main(args: Array[String]) {

    //Setting the logging to ERROR
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    val outputLocation = args(0)

    val conf = new SparkConf().setAppName("Flights Example")

    // sc is the SparkContext.
    val sc = new SparkContext(conf)

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val parquetFile = hiveContext.parquetFile("s3://us-east-1.elasticmapreduce.samples/flightdata/input/")

    //Parquet files can also be registered as tables and then used in SQL statements.
    parquetFile.registerTempTable("flights")

    runSqlQueries(outputLocation, hiveContext)


  }
}