package com.github.spirom.sparkflights.etl

import com.github.spirom.sparkflights.FlightsDataSchema
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object ParquetBuilderMain {

  val logger = Logger.getLogger(getClass.getName)

  logger.setLevel(Level.ALL)

  val options = new BuilderOptionsConfig()

  def main(args: Array[String]): Unit = {
    options.parser.parse(args, options) match {
      case Some(parsedOptions) => {
        val conf = if (parsedOptions.local) {
          logger.info("Local configuration")
          new SparkConf().setAppName("Flights Example").setMaster("local[4]")
        } else {
          logger.info("running in a cluster")
          new SparkConf().setAppName("Flights Example")
        }
        val csvPath = "/mnt/data2/FlightData/On_Time_On_Time_Performance_2010_1/On_Time_On_Time_Performance_2010_1.csv"
        logger.info(s"SparkFlights: Reading CSV data from $csvPath")

        val sc = new SparkContext(conf)

        val sqlContext = new SQLContext(sc)

        val data = sqlContext.read
                .format("com.databricks.spark.csv")
                .option("header", "true") // Use first line of all files as header
                .option("inferSchema", "false") // Automatically infer data types
                .schema(FlightsDataSchema.schema)
          .load(csvPath)

        println(data.rdd.count())

        data.show(1)

        data.printSchema()
        data.write.mode("append").parquet("/mnt/data2/FlightDataParquet/flights")
      }


    case None =>
      logger.fatal("Bad command line")



    }
  }
}
