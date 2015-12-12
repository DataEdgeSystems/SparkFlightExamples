package com.github.spirom.sparkflights.etl

import java.net.URI;

case class BuilderOptionsConfig
(
  runAll: Boolean = false,
  local: Boolean = false,
  out:URI = new URI("."),
  parquet:URI = new URI("."),
  csv:URI = new URI("."),
  run:Seq[String] = Seq()
  ) {

  val parser = new scopt.OptionParser[BuilderOptionsConfig]("FlightExample") {
    head("FlightExample", "1.5")

    opt[Unit]("local") action { (_, c) =>
      c.copy(local = true) } text("Local, embedded Spark for testing")

    opt[URI]('o', "out") required() valueName("<URI>") action { (x, c) =>
      c.copy(out = x) } text("required URI of output destination")

    opt[URI]("csv") optional() valueName("<URI>") action { (x, c) =>
      c.copy(csv = x) } text("URI of CSV input")

    opt[URI]("parquet") optional() valueName("<URI>") action { (x, c) =>
      c.copy(parquet = x) } text("URI of Parquet input")
  }
}
