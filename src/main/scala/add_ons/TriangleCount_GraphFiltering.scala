package add_ons

import org.apache.spark.graphx._
import scala.collection.mutable

class TriangleCount_GraphFiltering extends Serializable {

  def run(queryTimeInterval: (Long, Long), graph: Graph[Int, (Long, Long)]): VertexRDD[Double] = {
    // Standardize neighbor set collection for better performance
    val nbrSets = GraphUtils.getNeighborSets(graph)

    val setGraph = graph.outerJoinVertices(nbrSets) {
      (_, _, optSet) => optSet.getOrElse(mutable.HashMap.empty[VertexId, (Long, Long)])
    }

    // Call centralized logic with the query interval
    GraphUtils.computeTriangleScores(setGraph, Some(queryTimeInterval))
  }
}

object TriangleCount_GraphFiltering {
  def main(args: Array[String]): Unit = {
    val (sc, spark) = GraphUtils.setupSpark("TriangleCount_GraphFiltering")
    val queryTimeInterval: (Long, Long) = (2L, 2L)

    val edges = if (args.length > 0) GraphUtils.loadEdges(sc, args(0)) else GraphUtils.loadEdges(sc)
    val startTime = System.currentTimeMillis()

    val filteredEdges = edges.filter(e => GraphUtils.intersectIntervals(e.attr, queryTimeInterval).isDefined)
    val scores = new TriangleCount_GraphFiltering().run(queryTimeInterval, Graph.fromEdges(filteredEdges, 1))

    val total = if (scores.isEmpty()) 0.0 else scores.map(_._2).sum() / 3.0
    println(s"Total triangles: $total")
    println(s"Time taken: ${System.currentTimeMillis() - startTime} ms")

    GraphUtils.close(sc, spark)
  }
}