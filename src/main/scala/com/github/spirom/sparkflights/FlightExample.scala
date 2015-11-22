package com.github.spirom.sparkflights

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SQLContext, DataFrame}
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

class fFlightsMain {}

object FlightsMain {

  val logger = Logger.getLogger(getClass.getName)

  /*

  root
 |-- year: integer (nullable = true)
 |-- quarter: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- dayofmonth: integer (nullable = true)
 |-- dayofweek: integer (nullable = true)
 |-- flightdate: string (nullable = true)
 |-- uniquecarrier: string (nullable = true)
 |-- airlineid: integer (nullable = true)
 |-- carrier: string (nullable = true)
 |-- tailnum: string (nullable = true)
 |-- flightnum: integer (nullable = true)
 |-- originairportid: integer (nullable = true)
 |-- originairportseqid: integer (nullable = true)
 |-- origincitymarketid: integer (nullable = true)
 |-- origin: string (nullable = true)
 |-- origincityname: string (nullable = true)
 |-- originstate: string (nullable = true)
 |-- originstatefips: integer (nullable = true)
 |-- originstatename: string (nullable = true)
 |-- originwac: integer (nullable = true)
 |-- destairportid: integer (nullable = true)
 |-- destairportseqid: integer (nullable = true)
 |-- destcitymarketid: integer (nullable = true)
 |-- dest: string (nullable = true)
 |-- destcityname: string (nullable = true)
 |-- deststate: string (nullable = true)
 |-- deststatefips: integer (nullable = true)
 |-- deststatename: string (nullable = true)
 |-- destwac: integer (nullable = true)
 |-- crsdeptime: integer (nullable = true)
 |-- deptime: integer (nullable = true)
 |-- depdelay: integer (nullable = true)
 |-- depdelayminutes: integer (nullable = true)
 |-- depdel15: integer (nullable = true)
 |-- departuredelaygroups: integer (nullable = true)
 |-- deptimeblk: integer (nullable = true)
 |-- taxiout: integer (nullable = true)
 |-- wheelsoff: integer (nullable = true)
 |-- wheelson: integer (nullable = true)
 |-- taxiin: integer (nullable = true)
 |-- crsarrtime: integer (nullable = true)
 |-- arrtime: integer (nullable = true)
 |-- arrdelay: integer (nullable = true)
 |-- arrdelayminutes: integer (nullable = true)
 |-- arrdel15: integer (nullable = true)
 |-- arrivaldelaygroups: integer (nullable = true)
 |-- arrtimeblk: string (nullable = true)
 |-- cancelled: integer (nullable = true)
 |-- cancellationcode: integer (nullable = true)
 |-- diverted: integer (nullable = true)
 |-- crselapsedtime: integer (nullable = true)
 |-- actualelapsedtime: integer (nullable = true)
 |-- airtime: integer (nullable = true)
 |-- flights: integer (nullable = true)
 |-- distance: integer (nullable = true)
 |-- distancegroup: integer (nullable = true)
 |-- carrierdelay: integer (nullable = true)
 |-- weatherdelay: integer (nullable = true)
 |-- nasdelay: integer (nullable = true)
 |-- securitydelay: integer (nullable = true)
 |-- lateaircraftdelay: integer (nullable = true)
 |-- firstdeptime: integer (nullable = true)
 |-- totaladdgtime: integer (nullable = true)
 |-- longestaddgtime: integer (nullable = true)
 |-- divairportlandings: integer (nullable = true)
 |-- divreacheddest: integer (nullable = true)
 |-- divactualelapsedtime: integer (nullable = true)
 |-- divarrdelay: integer (nullable = true)
 |-- divdistance: integer (nullable = true)
 |-- div1airport: integer (nullable = true)
 |-- div1airportid: integer (nullable = true)
 |-- div1airportseqid: integer (nullable = true)
 |-- div1wheelson: integer (nullable = true)
 |-- div1totalgtime: integer (nullable = true)
 |-- div1longestgtime: integer (nullable = true)
 |-- div1wheelsoff: integer (nullable = true)
 |-- div1tailnum: integer (nullable = true)
 |-- div2airport: integer (nullable = true)
 |-- div2airportid: integer (nullable = true)
 |-- div2airportseqid: integer (nullable = true)
 |-- div2wheelson: integer (nullable = true)
 |-- div2totalgtime: integer (nullable = true)
 |-- div2longestgtime: integer (nullable = true)
 |-- div2wheelsoff: integer (nullable = true)
 |-- div2tailnum: integer (nullable = true)
 |-- div3airport: integer (nullable = true)
 |-- div3airportid: integer (nullable = true)
 |-- div3airportseqid: integer (nullable = true)
 |-- div3wheelson: integer (nullable = true)
 |-- div3totalgtime: integer (nullable = true)
 |-- div3longestgtime: integer (nullable = true)
 |-- div3wheelsoff: integer (nullable = true)
 |-- div3tailnum: integer (nullable = true)
 |-- div4airport: integer (nullable = true)
 |-- div4airportid: integer (nullable = true)
 |-- div4airportseqid: integer (nullable = true)
 |-- div4wheelson: integer (nullable = true)
 |-- div4totalgtime: integer (nullable = true)
 |-- div4longestgtime: integer (nullable = true)
 |-- div4wheelsoff: integer (nullable = true)
 |-- div4tailnum: integer (nullable = true)
 |-- div5airport: integer (nullable = true)
 |-- div5airportid: integer (nullable = true)
 |-- div5airportseqid: integer (nullable = true)
 |-- div5wheelson: integer (nullable = true)
 |-- div5totalgtime: integer (nullable = true)
 |-- div5longestgtime: integer (nullable = true)
 |-- div5wheelsoff: integer (nullable = true)
 |-- div5tailnum: integer (nullable = true)

  */

  //val pw = new PrintWriter(new File("/tmp/flightlog.txt" ))


  def runSqlQueries(outputLocation: String, sqlContext: SQLContext): Unit = {
    //Top 10 airports with the most departures since 2000
    //val topDepartures = hiveContext.sql("SELECT origin, count(*) AS total_departures FROM flights WHERE year >= '2000' GROUP BY origin ORDER BY total_departures DESC LIMIT 10")
    //topDepartures.rdd.saveAsTextFile(s"$outputLocation/top_departures")

    //val all = hiveContext.sql("SELECT * FROM flights LIMIT 10")
    //all.rdd.saveAsTextFile(s"$outputLocation/all_columns")
    //logger.info(all.schema.treeString)

    //val allCount = sqlContext.sql("SELECT count(*) as total_rows FROM flights")
    //allCount.rdd.saveAsTextFile(s"$outputLocation/all_rows")

    try {
      val years = sqlContext.sql("SELECT DISTINCT year FROM flights ORDER BY year")
      years.rdd.saveAsTextFile("s3://spirom-spark-output/flights/all_years_text")
      years.write.mode(SaveMode.Overwrite).save("s3://spirom-spark-output/flights/all_years")
    } catch {
      case e:Throwable => {
        //pw.write("caught an exception\n")
        //pw.write(e.getMessage + "\n")
        //pw.write(e.getStackTraceString + "\n")
      }

    }

    /*
    //Top 10 airports with the most departure delays over 15 minutes since 2000
    val shortDepDelay = sqlContext.sql("SELECT origin, count(depDelay) as cnt FROM flights WHERE depDelay >= '15' AND year >= '2000' GROUP BY origin ORDER BY cnt DESC LIMIT 10")
    shortDepDelay.rdd.saveAsTextFile(s"$outputLocation/top_short_delays")

    //Top 10 airports with the most departure delays over 60 minutes since 2000
    val longDepDelay = sqlContext.sql("SELECT origin, count(depDelay) AS total_delays FROM flights WHERE depDelay > '60' AND year >= '2000' GROUP BY origin ORDER BY total_delays DESC LIMIT 10")
    longDepDelay.rdd.saveAsTextFile(s"$outputLocation/top_long_delays")

    //Top 10 airports with the most departure cancellations since 2000
    val topCancel = sqlContext.sql("SELECT origin, count(cancelled) AS total_cancellations FROM flights WHERE cancelled = '1' AND year >= '2000' GROUP BY origin ORDER BY total_cancellations DESC LIMIT 10")
    topCancel.rdd.saveAsTextFile(s"$outputLocation/top_cancellations")

    //Rank of the worst quarter of the year for departure cancellations
    val quarterCancel = sqlContext.sql("SELECT quarter, count(cancelled) AS total_cancellations FROM flights WHERE cancelled = '1' GROUP BY quarter ORDER BY total_cancellations DESC LIMIT 10")
    quarterCancel.rdd.saveAsTextFile(s"$outputLocation/rank_quarter_cancellations")

    //Top 10 most popular flight routes since 2000
    val popularFlights = sqlContext.sql("SELECT origin, dest, count(*) AS total_flights FROM flights WHERE year >= '2000' GROUP BY origin, dest ORDER BY total_flights DESC LIMIT 10")
    popularFlights.rdd.saveAsTextFile(s"$outputLocation/popular_flights")
    */
  }

  def coreQueries(all: DataFrame): Unit = {
    val tripples:RDD[(String, Int, Int)] = all.select("origin", "depDelay", "year").map(r =>
     (r.getString(0), r.getInt(1), r.getInt(2)))
    val originWithCount = tripples.filter(r => (r._2 > 15) && (r._3 > 2000)).groupBy(r => r._1).map(r => (r._1,r._2.size))
    val topTen = originWithCount.sortBy(p => p._2, false).take(10)


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

    logger.setLevel(Level.ALL)

    val outputLocation = args(0)

    val conf = new SparkConf().setAppName("Flights Example")

    // sc is the SparkContext.
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    //pw.write("loading\n")
    logger.info("SparkFlights: Reading Parquet data")

    val all = sqlContext.read.parquet("s3://us-east-1.elasticmapreduce.samples/flightdata/input/")

    //coreQueries(all)

    //Parquet files can also be registered as tables and then used in SQL statements.
    all.registerTempTable("flights")

    logger.info("SparkFlights: Starting to run queries")
    //pw.write("querying\n")


    runSqlQueries(outputLocation, sqlContext)

    //pw.write("done\n")

    logger.info("SparkFlights: All queries done")
  }
}