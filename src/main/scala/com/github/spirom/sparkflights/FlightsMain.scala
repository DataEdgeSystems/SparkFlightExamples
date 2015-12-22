package com.github.spirom.sparkflights

import com.github.spirom.sparkflights.config.OptionsConfig
import com.github.spirom.sparkflights.experiments._
import com.github.spirom.sparkflights.fw.{Registry, Runner}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SQLContext, DataFrame}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._

class Flights(args: Array[String]) {

  val logger = Logger.getLogger(getClass.getName)

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
            .schema(FlightsDataSchema.schema)
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

          //Parquet files can also be registered as tables and then used in SQL statements.
          data.registerTempTable("flights")

          logger.info("SparkFlights: Starting to run queries")

          val registry = new Registry()

          // TODO: currently runs in random order
          registry.add(new YearsCoveredSQL(sqlContext))
          registry.add(new YearsCoveredCore(sc))
          registry.add(new TopAirportsByCancellationsSQL(sqlContext))
          registry.add(new TopAirportsByDeparturesSQL(sqlContext))
          registry.add(new TopAirportsByShortDelaysSQL(sqlContext))
          registry.add(new TopAirportsByLongDelaysSQL(sqlContext))
          registry.add(new TopAirportsByLongDelaysCore(sc))
          registry.add(new TopAirportsByLongDelaysPercentCore(sc))
          registry.add(new TopAirportsByAnnualDeparturesCore(sc))
          registry.add(new TopQuartersByCancellationsSQL(sqlContext))
          registry.add(new MostPopularRoutesSQL(sqlContext))

          // TODO: the first elapsed time probably includes loading the data
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