package com.github.spirom.sparkflights.etl

import com.github.spirom.sparkflights.fw.RDDLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


object ParquetSubsetMain {

  val logger = Logger.getLogger(getClass.getName)

  logger.setLevel(Level.ALL)

  def main(args: Array[String]): Unit = {

    val from = args(0)
    val to = args(1)

    logger.info("running in a cluster")
    val conf = new SparkConf().setAppName("Flights Example")

    logger.info(s"SparkFlights: Reading data from $from")
    logger.info(s"SparkFlights: Writing subset data to $to")

    val sc = new SparkContext(conf)

    val rddLogger = new RDDLogger("/logger", sc)
    //rddLogger.enabled = true

    rddLogger.log(from)
    rddLogger.log(to)

    val sqlContext = new SQLContext(sc)

    val data = sqlContext.read.parquet(from)

    data.registerTempTable("flights")

    val subset = sqlContext.sql("SELECT * FROM flights WHERE year >= '2013' AND year <= '2014' ")

    subset.write.mode("append").parquet(to)

  }
}
