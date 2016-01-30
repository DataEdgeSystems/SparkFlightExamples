
# This project

This project is an exploration of how to perform basic data analytics efficiently
using core Apache Spark directly through Scala code. It was initially inspired by
a [blog post by Jon Fritz](https://aws.amazon.com/blogs/aws/new-apache-spark-on-amazon-emr/)
on the [Amazon Web Services official blog](https://aws.amazon.com/blogs/aws/).
The project described there simply runs quite basic Spark SQL queries against a well
known data set (described below) on US airline flight data. The original queries are
reproduced here, reformatted and commented rather more pedantically to emphasize what is
being computed.

This project contains a framework for registering many queries,
whether written in Spark SQL or core Spark,
and capturing the results and performance characteristics separately,
with a simple command line interface. The project goals are:

1. Compare the performance of Spark SQL and core Spark queries
2. Explore techniques for writing core Spark code that is both efficient and tolerably readable
3. Have some fun with the interactions between core Spark and data science, focusing on queries
   that actually expose interesting properties of the data.

# The data

## Official site

These examples analyze the data published by the US Government (specifically,
the Bureau of Transport Statistics in the Department of Transportation) as
[Airline On-Time Performance and Causes of Flight Delays: On_Time Data]
(https://catalog.data.gov/dataset/airline-on-time-performance-and-causes-of-flight-delays).

## This instance of the data

This data is available in Parquet format on Amazon S3 as a sample for this
[blog post](https://aws.amazon.com/blogs/aws/new-apache-spark-on-amazon-emr/).
The code uses that instance of the data.

## Interesting properties

The data spans from 1987 to 2015. There are 162,212,419 rows.
It appears to contain all the columns in the official data set.

| Statistic | Value |
| --------- | -----:|
| Flights | 162,212,419 |
| Aircraft Tail Numbers | 14,858 |
| Records with missing or unknown tail numbers | 38,687,083 |
| Maximum number of flights for a single tail number | 43,989 |
| Canceled Flights | |
| Delayed Flights > 1 hour | |
| Airports | 388 |
| Airlines (Unique Carrier) | 31 |
| Airport (Origin,Destination) pairs with flights | 9554 |
| Airport pair with the most flights | SFO to LAX (426,506) |


# Building and running

## Building

## Command line(s)

### ParquetSubsetMain

This is for creating smaller Parquet extracts from the full data set.
One application is to create a data set that is small enough to download
to a PC or laptop.

    java com.github.spirom.sparkflights.etl.ParquetSubsetMain <sourceParquetUrl> <destinationParquetUrl>

### ParquetBuilderMain

Note: this is not ready for use.

This is intended for creating your own Parquet data sets from the CSV form of
the on-time performance data as provided by the Bureau of
Transportation Statistics.

### FlightsMain

    java com.github.spirom.sparkflights.FlightsMain [options]

      --sanity <URI>
            Sanity check: just output an RDD to this URI and exit
      --local
            Local, embedded Spark for testing
      --list
            List the available experiments
      -o <URI> | --out <URI>
            required URI of output destination
      --csv <URI>
            URI of CSV input
      --parquet <URI>
            URI of Parquet input
      --run <name1>,<name2>,...
              Name of experiments to run (otherwise run all registered

# Finding the Output

As the framework executes experiments, it produces a directory tree with
summary results as well as the actual output of the various Spark computations.
The tree looks like this:

    <specified output directory>
    |
    |-- <date and time at job start>
        |
        |-- summary
        |   |
        |   |-- executions
        |   |
        |   |-- unknown
        |
        |-- 00001_<Experiment Name 1>
        |   |
        |   |-- <Result 1>
        |   |
        |   |-- ...
        |   |
        |   |-- <Result n_1>
        |
        |-- ...
        |
        |-- 0000k_<Experiment Name k>
            |
            |-- <Result 1>
            |
            |-- ...
            |
            |-- <Result n_k>

Each of the leaf nodes int he tree are HDFS "files",
with their various underlying "part", "SUCCESS" and ".crc" files.

The "executions" node contains a summary of what ran when, how
long it took, and whether it succeeded. Stack traces are included
in case of a failure.

The "unknown" node lists experiments explicitly specified ont he command line that
could not be found int he registry.

The experiment nodes are numbered to show the sequence in which they ran and to
accommodate the same experiment being run more than once.

The number of result nodes under an experiment varies,
and is determined by the implementation of each individual experiment.

# The Core Spark Queries

## Annotated List of Queries

| Query | Purpose |
| ----- | ------- |
| [YearsCoveredCore](src/main/scala/com/github/spirom/sparkflights/experiments/YearsCoveredCore.scala) | Very simply, determine which years the data covers -- every year produced is guaranteed to have at least one flight record in the data |
| [TopAirportsByAnnualDeparturesCore](src/main/scala/com/github/spirom/sparkflights/experiments/TopAirportsByAnnualDeparturesCore.scala) | Which airports have the most departures -- using the average yearly count.  |
| [TopAirportsByLongDelaysCore](src/main/scala/com/github/spirom/sparkflights/experiments/TopAirportsByLongDelaysCore.scala) | Which airports have the highest absolute number of delays over an hour. |
| [TopAirportsByLongDelaysPercentCore](src/main/scala/com/github/spirom/sparkflights/experiments/TopAirportsByLongDelaysPercentCore.scala) | Which airports have the highest percentage of flights delayed mroe than an hour -- this is the less naive version as it adjusts for how busy the airport is. |
| [TailNumbersCore](src/main/scala/com/github/spirom/sparkflights/experiments/TailNumbersCore.scala) | Number of flights per tail number and count of tail numbers |
| [AirlinesByTotalTailNumbersCore](src/main/scala/com/github/spirom/sparkflights/experiments/AirlinesByTotalTailNumbersCore.scala) | Total number of tail numbers for each airline |
| [TopAirlinesByAnnualDeparturesCore](src/main/scala/com/github/spirom/sparkflights/experiments/TopAirlinesByAnnualDeparturesCore.scala) | Which airlines have the most departures |


## Programming Approach

## Performance

(Watch this space.)