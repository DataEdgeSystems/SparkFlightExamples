package com.github.spirom.sparkflights.util

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

//
// Encapsulates PairRDDFunctions.aggregateByKey and its 'seqOp' and 'combOp'
// for the simple case of counting records.
//
class PairAggregateCounter[K:ClassTag]
  extends Serializable
   {

     private def initial: Int = 0

     private def seq(acc: Int, value: Int): Int = {
       acc + 1
     }

     private def comb(acc1: Int, acc2: Int): Int = {
       acc1 + acc2
     }

     def aggregateByKey(pairs: RDD[(K, Int)]): RDD[(K, Int)] =
     {
       pairs.aggregateByKey(initial)(seq, comb)
     }
   }