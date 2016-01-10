package com.github.spirom.sparkflights.fw

import org.apache.spark.SparkContext


class RDDLogger(root: String, sc: SparkContext) {

  var count = 0

  val enabled = false

  def log(msg: String) : Unit = {
    if (enabled) {
      count += 1
      sc.parallelize(Seq(msg), 1).saveAsTextFile(root + "/" + count)
    }
  }

}
