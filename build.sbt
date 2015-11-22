name := "SparkFlightExamples"

version := "1.5"

scalaVersion := "2.10.4"

scalacOptions in ThisBuild ++= Seq("-deprecation")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.2"

libraryDependencies += "com.github.scopt" % "scopt_2.10" % "3.3.0"

