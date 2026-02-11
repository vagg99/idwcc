package add_ons

import org.apache.spark.graphx._

class Thresholding extends Serializable {
  def run(queryTimeInterval: (Long, Long), graph: Graph[Int, (Long, Long)]): VertexRDD[Double] = {
    GraphUtils.executeTriangleCounting(graph, Some(queryTimeInterval))
  }
}

object Thresholding {
  def main(args: Array[String]): Unit = {
    val (sc, spark) = GraphUtils.setupSpark("Thresholding")
    val edges = if (args.length > 0) GraphUtils.loadEdges(sc, args(0)) else GraphUtils.loadEdges(sc)

    val algorithmStartTime = System.currentTimeMillis()
    val queryTimeInterval = (2L, 2L)
    val overlapPercentage = 0.0
    val queryLen = queryTimeInterval._2 - queryTimeInterval._1 + 1

    val filteredEdges = edges.flatMap { edge =>
      GraphUtils.intersectIntervals(edge.attr, queryTimeInterval) match {
        case Some((start, end)) =>
          val overlapPct = ((end - start + 1).toDouble / queryLen) * 100.0
          if (overlapPct >= overlapPercentage) Some(edge.copy(attr = (start, end))) else None
        case None => None
      }
    }

    val subgraph = Graph.fromEdges(filteredEdges, defaultValue = 1)
    val scores = new Thresholding().run(queryTimeInterval, subgraph)

    val totalTriangles = if (!scores.isEmpty()) scores.map(_._2).sum() / 3.0 else 0.0
    println(s"Total number of triangles: $totalTriangles")
    println(s"Time taken: ${System.currentTimeMillis() - algorithmStartTime} ms")

    GraphUtils.close(sc, spark)
  }
}