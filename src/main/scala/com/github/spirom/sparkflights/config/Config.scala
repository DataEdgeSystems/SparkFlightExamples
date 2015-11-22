package com.github.spirom.sparkflights.config

import java.net.URI;

case class Config(
                   all: Boolean = false,
                   out:URI = new URI("."),
                   parquet:URI = new URI("."),
                   csv:URI = new URI("."),
                   run:Seq[String] = Seq()
                   ) {

}
