package add_ons

import org.apache.spark.graphx._

/**
 * This is the full extent of the `Tr-Thresholding`
 * algorithm, as analyzed in my diploma thesis.
 */
class Thresholding extends Serializable {
  def run(graph: Graph[Int, (Long, Long)]): VertexRDD[Double] = {
    GraphUtils.executeTriangleCounting(graph)
  }
}

object Thresholding {
  def main(args: Array[String]): Unit = {
    val (sc, spark) = GraphUtils.setupSpark("Thresholding")
    val edges = GraphUtils.loadEdgesFromArgs(sc, args)

    val queryTimeInterval = GraphUtils.QUERY_TIME_INTERVAL
    val queryLen = GraphUtils.queryLen

    val filteredEdges = edges.flatMap { edge =>
      GraphUtils.intersectIntervals(edge.attr, queryTimeInterval)
        .map { case (start, end) => edge.copy(attr = (start, end)) }
        .filter { e =>
          // Threshold set as percentage at the end of this line
          // (e.g. '40.0' for an expected 40% threshold)
          val threshold = 40.0
          ((e.attr._2 - e.attr._1 + 1).toDouble / queryLen) * 100.0 >= threshold
        }
    }

    val (subgraph, startTime) = GraphUtils.createGraphAndLog(filteredEdges)
    val scores = new Thresholding().run(subgraph)

    GraphUtils.aggregateAndReport(scores, startTime, sc, spark)
  }
}
