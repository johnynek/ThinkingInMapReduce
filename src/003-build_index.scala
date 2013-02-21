#!/bin/sh
exec scala -savecompiled "$0" "$@"
!#

// Toy Map Reduce framework:
class MapReduce[T,K,V,R](flatMapFn: (T) => Iterable[(K,V)], reduceFn: ((K,Iterable[V])) => R) {

  def apply(input: Iterable[T]): Map[K,R] = {
    // Apply the flatMap function:
    val mapped: Iterable[(K,V)] = input.flatMap(flatMapFn)
    // do the shuffle (in distributed systems, sets of keys are handled by different workers
    val shuffled: Map[K,Iterable[(K,V)]] = mapped.groupBy { _._1 }
    // Just keep the V values in the second part of the Map (to clean up the function we pass in)
    val values: Map[K, (K,Iterable[V])] = shuffled.map { case (k,kvs) => (k, (k, kvs.map { _._2 })) }
    // apply reduce:
    values.mapValues(reduceFn)
  }
}

/*
 * Now let's use our MapReduce framework to do wordcount
 */

// Map onto (Key, Value)
def mapFunction(line: (String, Int)): Iterable[(String,Int)] = {
  val lineNum = line._2
  line._1.split("""\s+""")
    .map { word => (word, lineNum) }
}

def reduceFunction(groupedWords: (String, Iterable[Int])): Set[Int] =
  // Build the set of lines a word is on:
  groupedWords._2.toSet

// Pass these functions to our job:
val myWordCountJob = new MapReduce(mapFunction _, reduceFunction _)

// Run the job:
//val input: Iterable[String] = scala.io.Source.stdin.getLines.toIterable
val filename = "alice.txt"
val input: Iterable[(String,Int)] = scala.io.Source.fromFile(filename).getLines.zipWithIndex.toIterable
//build the index
val index: Map[String, Set[Int]] = myWordCountJob.apply(input)

