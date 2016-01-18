package com.github.spirom.sparkflights.config

import java.net.URI;

case class OptionsConfig
(
  sanity:URI = new URI("."),
  runAll: Boolean = false,
  local: Boolean = false,
  list: Boolean = false,
  out:URI = new URI("."),
  parquet:URI = new URI("."),
  csv:URI = new URI("."),
  run:Seq[String] = Seq()
  ) {

  val parser = new scopt.OptionParser[OptionsConfig]("FlightsMain") {
    head("FlightsMain", "1.5")

    opt[URI]("sanity") optional() valueName("<URI>") action { (x, c) =>
      c.copy(sanity = x) } text("Sanity check: just output an RDD to this URI and exit")

    opt[Unit]("local") action { (_, c) =>
      c.copy(local = true) } text("Local, embedded Spark for testing")

    opt[Unit]("list") action { (_, c) =>
      c.copy(list = true) } text("List the available experiments")

    opt[URI]('o', "out") optional() valueName("<URI>") action { (x, c) =>
      c.copy(out = x) } text("required URI of output destination")

    opt[URI]("csv") optional() valueName("<URI>") action { (x, c) =>
      c.copy(csv = x) } text("URI of CSV input")

    opt[URI]("parquet") optional() valueName("<URI>") action { (x, c) =>
      c.copy(parquet = x) } text("URI of Parquet input")

    opt[Seq[String]]("run") optional() valueName("<name1>,<name2>,...") action { (x, c) =>
      c.copy(run = x) } text("Name of experiments to run (otherwise run all registered")
  }
}
