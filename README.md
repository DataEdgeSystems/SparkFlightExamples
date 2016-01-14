
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

# Building and running

## Building

## Command line(s)

### ParquetSubsetMain

This is for creating smaller Parquet extracts from the full data set.
One application is to create a data set that is small enough to download
to a PC or laptop.

    java ParquetSubsetMain <sourceParquetUrl> <destinationParquetUrl>

### ParquetBuilderMain

Note: this is not ready for use.

This is intended for creating your own Parquet data sets from the CSV form of
the on-time performance data as provided by the Bureau of
Transportation Statistics.

### FlightsMain

    java FlightsMain [options]

      --sanity <URI>
            Sanity check: just output an RDD to this URI and exit
      --local
            Local, embedded Spark for testing
      -o <URI> | --out <URI>
            required URI of output destination
      --csv <URI>
            URI of CSV input
      --parquet <URI>
            URI of Parquet input

# The Core Spark Queries

## Annotated List of Queries

| Query | Purpose |
| ----- | ------- |
| YearsCoveredCore | Very simply, determine which years the data covers -- every year produced is guaranteed to have at least one flight record int he data |
| TopAirportsByAnnualDeparturesCore | Which airports have the most departures -- using the average yearly count.  |
| TopAirportsByLongDelaysCore | Which airports have the highest absolute number of delays over an hour. |
| TopAirportsByLongDelaysPercentCore | Which airports have the highest percentage of flights delayed mroe than an hour -- this is the less naive version as it adjusts for how busy the airport is. |

## Programming Approach

## Performance

(Watch this space.)