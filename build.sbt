name := "SparkFlightExamples"

version := "1.5"

organization := "com.github.spirom"

scalaVersion := "2.10.4"

scalacOptions in ThisBuild ++= Seq("-deprecation")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.2" % "provided"


libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

libraryDependencies += "com.databricks" %% "spark-csv" % "1.3.0"

