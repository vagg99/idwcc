package add_ons

import org.apache.spark.graphx._

/**
 * This is the full extent of the `Tr-Thresholding`
 * algorithm, as analyzed in my diploma thesis.
 */
class Thresholding extends Serializable {
  def run(queryTimeInterval: (Long, Long), graph: Graph[Int, (Long, Long)]): VertexRDD[Double] = {
    GraphUtils.executeTriangleCounting(graph, Some(queryTimeInterval))
  }
}

object Thresholding {
  def main(args: Array[String]): Unit = {
    val (sc, spark) = GraphUtils.setupSpark("Thresholding")
    val edges = GraphUtils.loadEdgesFromArgs(sc, args)

    val queryTimeInterval = (2L, 2L)
    val queryLen = queryTimeInterval._2 - queryTimeInterval._1 + 1

    val filteredEdges = edges.flatMap { edge =>
      GraphUtils.intersectIntervals(edge.attr, queryTimeInterval).map { case (start, end) =>
        edge.copy(attr = (start, end))
      }.filter { e => ((e.attr._2 - e.attr._1 + 1).toDouble / queryLen) * 100.0 >= 0.0 }
    }

    val (subgraph, startTime) = GraphUtils.createGraphAndLog(filteredEdges)
    val scores = new Thresholding().run(queryTimeInterval, subgraph)

    GraphUtils.aggregateAndReport(scores, startTime, sc, spark)
  }
}