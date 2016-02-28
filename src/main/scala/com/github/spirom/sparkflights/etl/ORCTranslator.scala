package com.github.spirom.sparkflights.etl

import com.github.spirom.sparkflights.fw.RDDLogger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.hive.HiveContext

object ORCTranslator {

  def main(args: Array[String]): Unit = {

    val from = args(0)
    val to = args(1)

    val conf = new SparkConf().setAppName("ORC Translator").setMaster("local[4]")

    val sc = new SparkContext(conf)

    val rddLogger = new RDDLogger("/logger", sc)
    //rddLogger.enabled = true

    rddLogger.log(from)
    rddLogger.log(to)

    val sqlContext = new HiveContext(sc)

    val data = sqlContext.read.parquet(from)

    data.registerTempTable("flights")

    val subset = sqlContext.sql("SELECT * FROM flights")

    subset.write.format("orc").mode("append").save(to)

  }
}

