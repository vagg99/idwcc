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
    val edges = GraphUtils.loadEdgesFromArgs(sc, args)

    val (graph, startTime) = GraphUtils.createGraphAndLog(edges)
    val scores = new Brute_Force_with_intervals().run(graph)

    GraphUtils.aggregateAndReport(scores, startTime, sc, spark)
  }
}