
# This project

This project is an exploration of how to perform basic data analytics efficiently
using core Apache Spark directly through Scala code. It was initially inspired by
a [blog post by Jeff Barr](https://aws.amazon.com/blogs/aws/new-apache-spark-on-amazon-emr/)
on the [Amazon Web Services official blog](https://aws.amazon.com/blogs/aws/).
The project described there simply runs quite basic Spark SQL queries against a well
known data set (described below) on US airline flight data.

This project contains a framework for registering many queries,
whether written in Spark SQL or core Spark,
and capturing the results and performance characteristics separately,
with a simple command line interface. THe project goals are:

1. Compare the performance of Spark SQL and core Spark queries
2. Explore techniques for writing core Spark code that is both efficient and tolerably readable


# The data

These examples analyze the data published by the US Government as
[Airline On-Time Performance and Causes of Flight Delays: On_Time Data]
(https://catalog.data.gov/dataset/airline-on-time-performance-and-causes-of-flight-delays).

# This instance of the data

This data is available in Parquet format on Amazon S3 as a sample for this
[blog post](https://aws.amazon.com/blogs/aws/new-apache-spark-on-amazon-emr/).
The code uses that instance of the data.

The data spans from 1987 to 2015. There are 162,212,419 rows.
It appears to contain all the columns in the official datatset.

