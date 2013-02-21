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
////  Below here is the matrix stuff, above is just the Map/Reduce framework and the join
////
////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////

// Pass these functions to our job:
val mat1: Iterable[String] = scala.io.Source
  .fromFile("matrix1.txt")
  .getLines
  .toList

val mat2: Iterable[String] = scala.io.Source
  .fromFile("matrix2.txt")
  .getLines
  .toList

// Parse the line with the row being the key
def matrixParseRow(line: String): Iterable[(Int,(Int,Double))] = {
  line.split("""\s+""") match {
    case Array(row, col, value) => Iterable((row.toInt, (col.toInt, value.toDouble)))
    case _ => Iterable.empty
  }
}
// parse the line with col being the key
def matrixParseCol(line: String): Iterable[(Int,(Int,Double))] = {
  line.split("""\s+""") match {
    case Array(row, col, value) => Iterable((col.toInt, (row.toInt, value.toDouble)))
    case _ => Iterable.empty
  }
}

val parseRowJob = new MapReduce(matrixParseRow _, identityReducer[Int,(Int,Double)] _)
val parseColJob = new MapReduce(matrixParseCol _, identityReducer[Int,(Int,Double)] _)

val parsedMat1 = parseColJob.apply(mat1)
val parsedMat2 = parseRowJob.apply(mat2)

/* Matrix multiplication mat1 * mat2 =
 * \sum_k mat1(i,k) mat2(k,j)
 * so, join mat1 on column with mat2 on row and then
 * multiply:
 */
val joined: Iterable[(Int, (Iterable[Iterable[(Int,Double)]],Iterable[Iterable[(Int,Double)]]))] =
  join(parsedMat1, parsedMat2)

// Now make the key (row,col) and the reducer will do a dot product:
def makeRowCol(item: (Int, (Iterable[Iterable[(Int,Double)]],Iterable[Iterable[(Int,Double)]]))) = {
  val leftRow = item._2._1.head
  val rightCol = item._2._2.head
  for( (row,v) <- leftRow;
       (col,w) <- rightCol)
      yield ((row, col), v*w)
}
// The reducer here just adds everything up
val sumJob = new MapReduce(makeRowCol, { idxV : ((Int,Int),Iterable[Double]) => idxV._2.sum })

val product = sumJob.apply(joined)
product.toList.sorted.foreach { println(_) }
