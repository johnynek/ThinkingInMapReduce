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

def generatePageNumClick(size: Int): Iterable[String] = {
  import java.util.Random
  val seed = 1
  val pages = 100
  val rng = new Random(seed)
  (0 to size).map { idx =>
    // generate a random
    val page = rng.nextInt(pages)
    val clicked = rng.nextBoolean
    "%s %s".format(page, clicked)
  }
}

// Map onto (Key, Value)
def mapFunction(line: String): Iterable[(String,Int)] =
  line.split("""\s+""") match {
    case Array(page, clicked) =>
      val clickInt = if(java.lang.Boolean.valueOf(clicked)) 1 else 0
      Iterable((page, clickInt))
    case _ => Iterable.empty
  }

// Compute the average of a list of integers as a stream:
def reduceFunction(grouped: (String, Iterable[Int])): (Int, Double) = {
  val clickData = grouped._2
  // Compute the count and the ave at the same time:
  val initCountAve = (0, 0.0)
  clickData.foldLeft(initCountAve) { (cntAve, clicked) =>
    val (count, ave) = cntAve
    val newCount = count + 1
    // Can you think of a transformation of this to avoid doing ave * count?
    val newAve = (ave * count + clicked.toDouble)/newCount
    (newCount, newAve)
  }
}

// Pass these functions to our job:
val myWordCountJob = new MapReduce(mapFunction _, reduceFunction _)

// Run the job:
val input: Iterable[String] = generatePageNumClick(1000)
val result0 = myWordCountJob.apply(input)

// Second job: emit a key of 1, so everyone is sent to the same reduce function

def map2(pageCntRate: (String,(Int,Double))): Iterable[(Int,(Double,String))] =  {
  val (page, (cnt, rate)) = pageCntRate
  Iterable((1, (rate, page)))
}

val ITEMS_TO_KEEP = 10

def reduce2(data: (Int, Iterable[(Double, String)])): Set[(Double, String)] = {
  val initial = Set[(Double, String)]()
  data._2.foldLeft(initial) { (oldSet, v) =>
    val (rate, page) = v
    if(oldSet.size < 10) {
      //Keep it:
      oldSet + v
    }
    else if(oldSet.min._1 < rate) {
      // Remove the smallest and add this item:
      (oldSet - oldSet.min) + v
    }
    else {
      // Just keep the old set:
      oldSet
    }
  }
}

val totalCountJob = new MapReduce(map2 _, reduce2 _)
val result1 = totalCountJob.apply(result0)

//print it out:
result1.foreach { println(_) }


