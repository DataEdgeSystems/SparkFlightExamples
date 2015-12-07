package com.github.spirom.sparkflights.fw

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class Results {
  val results = new mutable.ListBuffer[ExperimentResult]()

  def add(result: ExperimentResult) : Unit = {
    results += result
  }

  def all = results.iterator

  private def asLoggableRDD(sc: SparkContext): RDD[(Boolean, String, String, String, String)] = {
    val raw = results.map(er => {
      (
        er.throwable.isEmpty,
        er.experiment.name,
        er.startString,
        er.finishString,
        er.throwable match {
          case Some(t) => t.getMessage()
          case None => ""
        }
        )
    })
    sc.parallelize(raw, 4)
  }

  def save(runOutputBase: String, sc: SparkContext) : Unit = {
    asLoggableRDD(sc).saveAsTextFile(runOutputBase + "/" + "summary")
  }


}