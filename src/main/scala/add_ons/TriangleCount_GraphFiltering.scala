package add_ons

import org.apache.spark.graphx._

/**
 * This is the full extent of the `Tr-Filtering`
 * algorithm, as analyzed in my diploma thesis.
 */
class TriangleCount_GraphFiltering extends Serializable {
  def run(graph: Graph[Int, (Long, Long)]): VertexRDD[Double] = {
    GraphUtils.executeTriangleCounting(graph)
  }
}

object TriangleCount_GraphFiltering {
  def main(args: Array[String]): Unit = {
    val (sc, spark) = GraphUtils.setupSpark("TriangleCount_GraphFiltering")

    val edges = GraphUtils.loadEdgesFromArgs(sc, args)
    val queryTimeInterval = GraphUtils.QUERY_TIME_INTERVAL

    val filteredEdges =
      edges.filter(e => GraphUtils.intersectIntervals(e.attr, queryTimeInterval).isDefined)

    val (subgraph, startTime) = GraphUtils.createGraphAndLog(filteredEdges)

    val scores = new TriangleCount_GraphFiltering().run(subgraph)
    GraphUtils.aggregateAndReport(scores, startTime, sc, spark)
  }
}
