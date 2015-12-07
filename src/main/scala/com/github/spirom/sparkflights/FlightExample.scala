package com.github.spirom.sparkflights

import com.github.spirom.sparkflights.config.OptionsConfig
import com.github.spirom.sparkflights.experiments.{YearsCoveredCore, YearsCoveredSQL}
import com.github.spirom.sparkflights.fw.{Registry, Runner}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SQLContext, DataFrame}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._

class Flights(args: Array[String]) {

  val logger = Logger.getLogger(getClass.getName)

  val schema = StructType(
    Seq(
      StructField("year", IntegerType),
      StructField("quarter", IntegerType),
      StructField("month", IntegerType),
      StructField("dayofmonth", IntegerType),
      StructField("dayofweek", IntegerType),
      StructField("flightdate", StringType),
      StructField("uniquecarrier", StringType),
      StructField("airlineid", IntegerType),
      StructField("carrier", StringType),
      StructField("tailnum", StringType),
      StructField("flightnum", IntegerType),
      StructField("originairportid", IntegerType),
      StructField("originairportseqid", IntegerType),
      StructField("origincitymarketid", IntegerType),
      StructField("origin", StringType),
      StructField("origincityname", StringType),
      StructField("originstate", StringType),
      StructField("originstatefips", IntegerType),
      StructField("originstatename", StringType),
      StructField("originwac", IntegerType),
      StructField("destairportid", IntegerType),
      StructField("destairportseqid", IntegerType),
      StructField("destcitymarketid", IntegerType),
      StructField("dest", StringType),
      StructField("destcityname", StringType),
      StructField("deststate", StringType),
      StructField("deststatefips", IntegerType),
      StructField("deststatename", StringType),
      StructField("destwac", IntegerType),
      StructField("crsdeptime", IntegerType),
      StructField("deptime", IntegerType),
      StructField("depdelay", IntegerType),
      StructField("depdelayminutes", IntegerType),
      StructField("depdel15", IntegerType),
      StructField("departuredelaygroups", IntegerType),
      StructField("deptimeblk", IntegerType),
      StructField("taxiout", IntegerType),
      StructField("wheelsoff", IntegerType),
      StructField("wheelson", IntegerType),
      StructField("taxiin", IntegerType),
      StructField("crsarrtime", IntegerType),
      StructField("arrtime", IntegerType),
      StructField("arrdelay", IntegerType),
      StructField("arrdelayminutes", IntegerType),
      StructField("arrdel15", IntegerType),
      StructField("arrivaldelaygroups", IntegerType),
      StructField("arrtimeblk", StringType),
      StructField("cancelled", IntegerType),
      StructField("cancellationcode", IntegerType),
      StructField("diverted", IntegerType),
      StructField("crselapsedtime", IntegerType),
      StructField("actualelapsedtime", IntegerType),
      StructField("airtime", IntegerType),
      StructField("flights", IntegerType),
      StructField("distance", IntegerType),
      StructField("distancegroup", IntegerType),
      StructField("carrierdelay", IntegerType),
      StructField("weatherdelay", IntegerType),
      StructField("nasdelay", IntegerType),
      StructField("securitydelay", IntegerType),
      StructField("lateaircraftdelay", IntegerType),
      StructField("firstdeptime", IntegerType),
      StructField("totaladdgtime", IntegerType),
      StructField("longestaddgtime", IntegerType),
      StructField("divairportlandings", IntegerType),
      StructField("divreacheddest", IntegerType),
      StructField("divactualelapsedtime", IntegerType),
      StructField("divarrdelay", IntegerType),
      StructField("divdistance", IntegerType),
      StructField("div1airport", IntegerType),
      StructField("div1airportid", IntegerType),
      StructField("div1airportseqid", IntegerType),
      StructField("div1wheelson", IntegerType),
      StructField("div1totalgtime", IntegerType),
      StructField("div1longestgtime", IntegerType),
      StructField("div1wheelsoff", IntegerType),
      StructField("div1tailnum", IntegerType),
      StructField("div2airport", IntegerType),
      StructField("div2airportid", IntegerType),
      StructField("div2airportseqid", IntegerType),
      StructField("div2wheelson", IntegerType),
      StructField("div2totalgtime", IntegerType),
      StructField("div2longestgtime", IntegerType),
      StructField("div2wheelsoff", IntegerType),
      StructField("div2tailnum", IntegerType),
      StructField("div3airport", IntegerType),
      StructField("div3airportid", IntegerType),
      StructField("div3airportseqid", IntegerType),
      StructField("div3wheelson", IntegerType),
      StructField("div3totalgtime", IntegerType),
      StructField("div3longestgtime", IntegerType),
      StructField("div3wheelsoff", IntegerType),
      StructField("div3tailnum", IntegerType),
      StructField("div4airport", IntegerType),
      StructField("div4airportid", IntegerType),
      StructField("div4airportseqid", IntegerType),
      StructField("div4wheelson", IntegerType),
      StructField("div4totalgtime", IntegerType),
      StructField("div4longestgtime", IntegerType),
      StructField("div4wheelsoff", IntegerType),
      StructField("div4tailnum", IntegerType),
      StructField("div5airport", IntegerType),
      StructField("div5airportid", IntegerType),
      StructField("div5airportseqid", IntegerType),
      StructField("div5wheelson", IntegerType),
      StructField("div5totalgtime", IntegerType),
      StructField("div5longestgtime", IntegerType),
      StructField("div5wheelsoff", IntegerType),
      StructField("div5tailnum", IntegerType)
    )
  )

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
      //years.rdd.saveAsTextFile(s"$outputLocation/flights/all_years_text")
      years.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(s"$outputLocation/all_years")
      years.show()
    } catch {
      case t: Throwable => {
        logger.error("query went bad", t)
      }

    }

    /*
    //Top 10 airports with the most departure delays over 15 minutes since 2000
    val shortDepDelay = sqlContext.sql("SELECT origin, count(depdelay) as cnt FROM flights WHERE depdelay >= '15' AND year >= '2000' GROUP BY origin ORDER BY cnt DESC LIMIT 10")
    shortDepDelay.rdd.saveAsTextFile(s"$outputLocation/top_short_delays")

    */
        /*
    //Top 10 airports with the most departure delays over 60 minutes since 2000
    val longDepDelay = sqlContext.sql("SELECT origin, count(depdelay) AS total_delays FROM flights WHERE depdelay > '60' AND year >= '2000' GROUP BY origin ORDER BY total_delays DESC LIMIT 10")
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
    val tripples: RDD[(String, Int, Int)] = all.select("origin", "depdelay", "year").map(r =>
      (r.getString(0), r.getInt(1), r.getInt(2)))
    val originWithCount = tripples.filter(r => (r._2 > 15) && (r._3 > 2000)).groupBy(r => r._1).map(r => (r._1, r._2.size))
    val topTen = originWithCount.sortBy(p => p._2, false).take(10)


  }

  //Setting the logging to ERROR
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

  logger.setLevel(Level.ALL)

  val options = new OptionsConfig()

  options.parser.parse(args, options) match {
    case Some(parsedOptions) => {
      val conf = if (parsedOptions.local) {
        logger.info("Local configuration")
        new SparkConf().setAppName("Flights Example").setMaster("local[4]")
      } else {
        logger.info("running in a cluster")
        new SparkConf().setAppName("Flights Example")
      }
      val sc = new SparkContext(conf)

      val sqlContext = new SQLContext(sc)

      val outputLocation = parsedOptions.out

      logger.info(s"Setting output destination to $outputLocation")

      val all = if (parsedOptions.csv.toString != ".") {
        logger.info(s"SparkFlights: Reading CSV data from ${parsedOptions.csv.toString}")
        val data = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "false") // Automatically infer data types
            .schema(schema)
          .load(parsedOptions.csv.toString)
        Some(data)
      } else if (parsedOptions.parquet.toString != ".") {
        logger.info(s"SparkFlights: Reading Parquet data from ${parsedOptions.parquet.toString}")
        Some(sqlContext.read.parquet(parsedOptions.parquet.toString))
        // "s3://us-east-1.elasticmapreduce.samples/flightdata/input/"
      } else {
        logger.fatal("no input specified")
        None
      }

      all match {
        case Some(data) => {
          data.printSchema()

          //coreQueries(all)

          //Parquet files can also be registered as tables and then used in SQL statements.
          data.registerTempTable("flights")

          logger.info("SparkFlights: Starting to run queries")

          //runSqlQueries(outputLocation.getPath, sqlContext)

          val registry = new Registry()

          // TODO: currently runs in random order
          registry.add(new YearsCoveredSQL(sqlContext))
          registry.add(new YearsCoveredCore(sc))

          val runner = new Runner(registry.getAll())
          runner.run(data, outputLocation.getPath)

          logger.info("SparkFlights: All queries done")

          runner.saveSummary(outputLocation.getPath, sc)
        }
        case None =>
      }

    }

    case None =>
      logger.fatal("Bad command line")



  }
}

  object FlightsMain {

    //
    def main(args: Array[String]) {
      val f = new Flights(args)

    }

}