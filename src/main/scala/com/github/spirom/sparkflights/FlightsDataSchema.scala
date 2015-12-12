package com.github.spirom.sparkflights

import org.apache.spark.sql.types._

object FlightsDataSchema {


  /*

"Year","Quarter","Month","DayofMonth","DayOfWeek",
"FlightDate","UniqueCarrier","AirlineID","Carrier","TailNum",
"FlightNum","Origin","OriginCityName","OriginState","OriginStateFips",
"OriginStateName","OriginWac","Dest","DestCityName","DestState",
"DestStateFips","DestStateName","DestWac",

"CRSDepTime","DepTime",
"DepDelay","DepDelayMinutes","DepDel15","DepartureDelayGroups","DepTimeBlk",

"TaxiOut","WheelsOff","WheelsOn","TaxiIn",

"CRSArrTime",
"ArrTime","ArrDelay","ArrDelayMinutes","ArrDel15","ArrivalDelayGroups",
"ArrTimeBlk",

"Cancelled","CancellationCode","Diverted",

"CRSElapsedTime",
"ActualElapsedTime","AirTime","Flights","Distance","DistanceGroup",

"CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay",

"FirstDepTime","TotalAddGTime","LongestAddGTime",

"DivAirportLandings","DivReachedDest",
"DivActualElapsedTime","DivArrDelay","DivDistance","Div1Airport","Div1WheelsOn",
"Div1TotalGTime","Div1LongestGTime","Div1WheelsOff","Div1TailNum"
,"Div2Airport","Div2WheelsOn","Div2TotalGTime","Div2LongestGTime","Div2WheelsOff
","Div2TailNum","Div3Airport","Div3WheelsOn","Div3TotalGTime","Div3LongestGTime"
,"Div3WheelsOff","Div3TailNum","Div4Airport","Div4WheelsOn","Div4TotalGTime","Di
v4LongestGTime","Div4WheelsOff","Div4TailNum","Div5Airport","Div5WheelsOn","Div5
TotalGTime","Div5LongestGTime","Div5WheelsOff","Div5TailNum",

   */


  val schema = StructType(
    Seq(
      StructField("year", IntegerType),
      StructField("quarter", IntegerType),
      StructField("month", IntegerType),
      StructField("dayofmonth", IntegerType),
      StructField("dayofweek", IntegerType),

      StructField("flightdate", StringType),
      StructField("uniquecarrier", StringType),
      StructField("airlineid", IntegerType),
      StructField("carrier", StringType),
      StructField("tailnum", StringType),

      StructField("flightnum", IntegerType),

      StructField("origin", StringType),
      StructField("origincityname", StringType),
      StructField("originstate", StringType),
      StructField("originstatefips", StringType),
      StructField("originstatename", StringType),
      //StructField("originairportid", StringType),
      //StructField("originairportseqid", IntegerType),
      //StructField("origincitymarketid", IntegerType),
      StructField("originwac", IntegerType),

      StructField("dest", StringType),
      StructField("destcityname", StringType),
      StructField("deststate", StringType),
      StructField("deststatefips", StringType),
      StructField("deststatename", StringType),
      //StructField("destairportid", StringType),
      //StructField("destairportseqid", IntegerType),
      //StructField("destcitymarketid", IntegerType),
      StructField("destwac", IntegerType),

      StructField("crsdeptime", StringType),
      StructField("deptime", StringType),
      StructField("depdelay", DoubleType),
      StructField("depdelayminutes", DoubleType),
      StructField("depdel15", StringType),
      StructField("departuredelaygroups", IntegerType),
      StructField("deptimeblk", StringType), //30

      StructField("taxiout", DoubleType),
      StructField("wheelsoff", IntegerType),
      StructField("wheelson", DoubleType),
      StructField("taxiin", DoubleType),

      StructField("crsarrtime", StringType),
      StructField("arrtime", StringType),
      StructField("arrdelay", DoubleType),
      StructField("arrdelayminutes", DoubleType),
      StructField("arrdel15", DoubleType),
      StructField("arrivaldelaygroups", IntegerType),
      StructField("arrtimeblk", StringType),

      StructField("cancelled", DoubleType),
      StructField("cancellationcode", StringType),
      StructField("diverted", DoubleType),

      StructField("crselapsedtime", DoubleType),
      StructField("actualelapsedtime", DoubleType),
      StructField("airtime", DoubleType),
      StructField("flights", DoubleType),
      StructField("distance", StringType),
      StructField("distancegroup", IntegerType), // 50

      StructField("carrierdelay", DoubleType),
      StructField("weatherdelay", DoubleType),
      StructField("nasdelay", DoubleType),
      StructField("securitydelay", DoubleType),
      StructField("lateaircraftdelay", DoubleType),

      StructField("firstdeptime", StringType),
      StructField("totaladdgtime", DoubleType),
      StructField("longestaddgtime", DoubleType),

      StructField("divairportlandings", IntegerType),
      StructField("divreacheddest", DoubleType),
      StructField("divactualelapsedtime", DoubleType),
      StructField("divarrdelay", DoubleType),
      StructField("divdistance", DoubleType), // 63?

      StructField("div1airport", StringType),
      //StructField("div1airportid", IntegerType),
      //StructField("div1airportseqid", IntegerType),
      StructField("div1wheelson", StringType),
      StructField("div1totalgtime", DoubleType),
      StructField("div1longestgtime", DoubleType),
      StructField("div1wheelsoff", StringType),
      StructField("div1tailnum", StringType),

      StructField("div2airport", StringType),
      //StructField("div2airportid", IntegerType),
      //StructField("div2airportseqid", IntegerType),
      StructField("div2wheelson", StringType),
      StructField("div2totalgtime", DoubleType),
      StructField("div2longestgtime", DoubleType),
      StructField("div2wheelsoff", StringType),
      StructField("div2tailnum", StringType),

      StructField("div3airport", StringType),
      //StructField("div3airportid", IntegerType),
      //StructField("div3airportseqid", IntegerType),
      StructField("div3wheelson", StringType),
      StructField("div3totalgtime", DoubleType),
      StructField("div3longestgtime", DoubleType),
      StructField("div3wheelsoff", StringType),
      StructField("div3tailnum", StringType),

      StructField("div4airport", StringType),
      //StructField("div4airportid", IntegerType),
      //StructField("div4airportseqid", IntegerType),
      StructField("div4wheelson", StringType),
      StructField("div4totalgtime", DoubleType),
      StructField("div4longestgtime", DoubleType),
      StructField("div4wheelsoff", StringType),
      StructField("div4tailnum", StringType),

      StructField("div5airport", StringType),
      //StructField("div5airportid", IntegerType),
      //StructField("div5airportseqid", IntegerType),
      StructField("div5wheelson", StringType),
      StructField("div5totalgtime", DoubleType),
      StructField("div5longestgtime", DoubleType),
      StructField("div5wheelsoff", StringType),
      StructField("div5tailnum", StringType)
    )
  )

}
