package add_ons

import org.apache.spark.graphx._

class Brute_Force_with_intervals extends Serializable {
  def run(graph: Graph[Int, (Long, Long)]): VertexRDD[Double] = {
    GraphUtils.executeTriangleCounting(graph)
  }
}

object Brute_Force_with_intervals {
  def main(args: Array[String]): Unit = {
    val (sc, spark) = GraphUtils.setupSpark("Brute_Force_with_intervals")
    val edges = if (args.length > 0) GraphUtils.loadEdges(sc, args(0)) else GraphUtils.loadEdges(sc)
    val graph = Graph.fromEdges(edges, defaultValue = 1)

    val startTime = System.currentTimeMillis()
    val scores = new Brute_Force_with_intervals().run(graph)

    val totalTriangles = if (scores.isEmpty()) 0.0 else scores.map(_._2).sum() / 3.0
    println(s"Total number of triangles: $totalTriangles")
    println(s"Time taken: ${System.currentTimeMillis() - startTime} ms")

    GraphUtils.close(sc, spark)
  }
}