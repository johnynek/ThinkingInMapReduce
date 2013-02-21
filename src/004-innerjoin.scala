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
 * Now let's use our MapReduce framework to do an inner join:
 */

def leftPrepareMapper[K,V,W](item: (K,V)): Iterable[(K,Either[V,W])] =
  Iterable((item._1, Left(item._2)))

def rightPrepareMapper[K,V,W](item: (K,W)): Iterable[(K,Either[V,W])] =
  Iterable((item._1, Right(item._2)))

// This reducer just passes everything out
def identityReducer[K,V](grouped: (K, Iterable[V])): Iterable[V] = grouped._2

// Now do the "join"
def joinReduceFunction[K,V,W](groupedWords: (K, Iterable[Either[V,W]])): (Iterable[V],Iterable[W])= {
  val vs = groupedWords._2.flatMap { either =>
    either match {
      case Left(v) => Iterable(v)
      case Right(w) => Iterable.empty
    }
  }
  val ws = groupedWords._2.flatMap { either =>
    either match {
      case Left(v) => Iterable.empty
      case Right(w) => Iterable(w)
    }
  }
  (vs, ws)
}

// Now create a two-step job to do a join:

def join[K,V,W](left: Iterable[(K,V)], right: Iterable[(K,W)]): Map[K,(Iterable[V], Iterable[W])] =
{
  val prepareLeft = new MapReduce[(K,V),K,Either[V,W],Iterable[Either[V,W]]](leftPrepareMapper _, identityReducer _)
  val prepareRight = new MapReduce[(K,W),K,Either[V,W],Iterable[Either[V,W]]](rightPrepareMapper _, identityReducer _)
  // Now we have to flatten the input:
  val joinJob = new MapReduce[(K,Iterable[Either[V,W]]),K,Either[V,W],(Iterable[V],Iterable[W])](
    {input => input._2.map { eith => (input._1, eith) }}, joinReduceFunction _)

  // Run the first job:
  val leftEither = prepareLeft.apply(left)
  val rightEither = prepareRight.apply(right)
  // Have to treat these like lists, not like Maps which are not "local" and replace keys
  val firstStepOutput = leftEither.toList ++ rightEither.toList
  // Run the second job
  joinJob.apply(firstStepOutput)
}
////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////
////
//// Below we use join to find any words that are appearing for the last time on that line
////
////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////


// Pass these functions to our job:
val data: Iterable[(String,Int)] = scala.io.Source
  .fromFile("alice.txt")
  .getLines
  .zipWithIndex
  .toList

// compute a table: (line-no, string)
val job1 = new MapReduce(
  { in: (String,Int) => Iterable(in.swap).filter { _._2.size > 0 } },
  // should be only one string per line, just take the first line:
  { grouped: (Int,Iterable[String]) => grouped._2.first } )
val output1: Iterable[(Int,String)] = job1.apply(data)

// compute a table: (word, last-line-seen):
val job2 = new MapReduce({ lineNo : (String,Int) =>
    val wordLine = lineNo._1
    //split it!
    wordLine.toLowerCase
      .split("""\s+""")
      .map { word => (word, lineNo._2) }
      .filter { wordLine => wordLine._1.size > 0 }
  },
  { wordLines : (String,Iterable[Int]) => wordLines._2.max })
val output2: Iterable[(String,Int)] = job2.apply(data)

// Now swap the line number to the key position:
val job3 = new MapReduce({ wordLast: (String,Int) => Iterable(wordLast.swap)},
  { identityReducer[Int,String] _ })
val output3: Iterable[(Int,Iterable[String])] = job3.apply(output2)

// Now run the join:
val results: Iterable[(Int, (Iterable[String], Iterable[Iterable[String]]))] =
  join(output1, output3)

results.foreach { println(_) }
