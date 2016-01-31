package com.github.spirom.sparkflights

import com.github.spirom.sparkflights.config.OptionsConfig
import com.github.spirom.sparkflights.experiments._
import com.github.spirom.sparkflights.fw.{RDDLogger, Registry, Runner}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


class Flights(args: Array[String]) {

  val logger = Logger.getLogger(getClass.getName)

  //Setting the logging to ERROR
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

  logger.setLevel(Level.ALL)

  val options = new OptionsConfig()

  def sanity(sc: SparkContext, output: String) : Unit = {
    sc.parallelize(Seq("Hello, world!"), 1).saveAsTextFile(output)
  }

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

      val rddLogger = new RDDLogger(outputLocation.toString, sc)

      logger.info(s"Setting output destination to $outputLocation")

      val all =
        if (parsedOptions.sanity.toString != ".") {
          sanity(sc, parsedOptions.sanity.toString)
          None
        } else if (parsedOptions.csv.toString != ".") {
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
          rddLogger.log("before " + parsedOptions.parquet.toString)
          val recs = sqlContext.read.parquet(parsedOptions.parquet.toString)
          rddLogger.log("after " + parsedOptions.parquet.toString)
          Some(recs)
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

          // TODO: currently runs in random order unless an actual
          // TODO: sequence is specified ont he command line
          registry.add(new YearsCoveredSQL(sqlContext))
          registry.add(new YearsCoveredCore(sc))
          registry.add(new AirportPairsCore(sc))
          registry.add(new TailNumbersCore(sc))
          registry.add(new TailNumbersCore2(sc))
          registry.add(new TailNumbersCore3(sc))
          registry.add(new TailNumbersSQL(sqlContext))
          registry.add(new TopAirportsByCancellationsSQL(sqlContext))
          registry.add(new TopAirportsByDeparturesSQL(sqlContext))
          registry.add(new TopAirportsByShortDelaysSQL(sqlContext))
          registry.add(new TopAirportsByLongDelaysSQL(sqlContext))
          registry.add(new TopAirportsByLongDelaysCore(sc))
          registry.add(new TopAirportsByLongDelaysPercentCore(sc))
          registry.add(new TopAirlinesByAnnualDeparturesCore(sc))
          registry.add(new TopAirportsByAnnualDeparturesCore(sc))
          registry.add(new AirlinesByTotalTailNumbersCore(sc))
          registry.add(new TopQuartersByCancellationsSQL(sqlContext))
          registry.add(new MostPopularRoutesSQL(sqlContext))

          val (experiments, unknownNames) =
            if (parsedOptions.run.nonEmpty) {
              val lookupResults =
                parsedOptions.run.map(name => {
                  val experiment = registry.lookup(name)
                  experiment match {
                    case None => Left(name)
                    case Some(e) => Right(e)
                  }
                })
              val unknownNames = lookupResults.map({
                case Left(name) => Some(name)
                case _ => None
              }).flatMap(e => e)
              val experiments = lookupResults.map({
                case Right(e) => Some(e)
                case _ => None
              }).flatMap(e => e)
              (experiments, unknownNames)
            } else {
              (registry.getAll(), Seq())
            }
          val runner = new Runner(experiments, sc, rddLogger, unknownNames)
          runner.run(data, outputLocation.toString)

          logger.info("SparkFlights: All queries done")

          runner.saveSummary(outputLocation.toString, sc)
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