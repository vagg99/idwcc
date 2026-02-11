package add_ons

import org.apache.spark.graphx._

class TriangleCount_GraphFiltering extends Serializable {
  def run(queryTimeInterval: (Long, Long), graph: Graph[Int, (Long, Long)]): VertexRDD[Double] = {

    val setGraph = GraphUtils.prepareSetGraph(graph)
    GraphUtils.computeTriangleScores(setGraph, Some(queryTimeInterval))
  }
}

object TriangleCount_GraphFiltering {
  def main(args: Array[String]): Unit = {
    val (sc, spark) = GraphUtils.setupSpark("TriangleCount_GraphFiltering")
    val queryTimeInterval = (2L, 2L)
    val edges = GraphUtils.loadEdgesFromArgs(sc, args)
    val startTime = System.currentTimeMillis()
    val filteredEdges = edges.filter(e => GraphUtils.intersectIntervals(e.attr, queryTimeInterval).isDefined)
    val subgraph = Graph.fromEdges(filteredEdges, 1)

    GraphUtils.log(s"vertices: ${subgraph.vertices.count()}, edges: ${subgraph.edges.count()}")

    val scores = new TriangleCount_GraphFiltering().run(queryTimeInterval, subgraph)

    GraphUtils.aggregateAndReport(scores, startTime, sc, spark)
  }
}