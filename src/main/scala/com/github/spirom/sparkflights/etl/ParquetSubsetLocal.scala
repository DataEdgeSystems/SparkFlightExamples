package com.github.spirom.sparkflights.etl

import com.github.spirom.sparkflights.fw.RDDLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object ParquetSubsetLocal {

  val logger = Logger.getLogger(getClass.getName)

  logger.setLevel(Level.ALL)

  def main(args: Array[String]): Unit = {

    val from = args(0)
    val to = args(1)

    logger.info("running locally")
    val conf = new SparkConf().setAppName("Flights Example").setMaster("local[4]")

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

    val subset = sqlContext.sql("SELECT * FROM flights WHERE origin = 'ORD' AND year = '2013' AND dayofmonth = '2' AND month = '1' ")

    subset.write.mode("append").parquet(to)

  }
}

