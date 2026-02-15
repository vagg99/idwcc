package add_ons

import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

/**
 * This is the full extent of the `Tr-Intervals`
 * algorithm, as analyzed in my diploma thesis.
 */
class Brute_Force(spark: SparkSession) extends Serializable {

  def run(graph: Graph[Int, (Long, Long)]): (VertexRDD[Double], List[GraphUtils.TriangleMetadata]) = {
    GraphUtils.log("Phase: Preprocessing - Counting Triangles")

    val setGraph = GraphUtils.prepareSetGraph(graph)
    val trianglesAccumulator = new GraphUtils.TriangleAccumulator()
    spark.sparkContext.register(trianglesAccumulator, "TrianglesAccumulator")

    val scores: VertexRDD[Double] = setGraph.aggregateMessages[Double](
      ctx => {
        val (smallSet, largeSet) = GraphUtils.orderSets(ctx.srcAttr, ctx.dstAttr)
        var triScore: Double = 0.0

        for (v <- smallSet.keys if v != ctx.srcId && v != ctx.dstId && largeSet.contains(v)) {
          triScore += 1.0
          List(ctx.srcId, v, ctx.dstId).sorted match {
            case a :: b :: c :: Nil =>
              trianglesAccumulator.add((a, b, c, ctx.srcAttr(v), ctx.dstAttr(v), ctx.srcAttr(ctx.dstId)))
            case _ =>
          }
        }
        ctx.sendToSrc(triScore / 2.0)
        ctx.sendToDst(triScore / 2.0)
      },
      (a, b) => a + b
    )

    scores.foreachPartition(_ => ())
    (scores, trianglesAccumulator.value)
  }
}

object Brute_Force {
  def main(args: Array[String]): Unit = {
    val (sc, spark) = GraphUtils.setupSpark("BruteForce")
    val rawEdges = GraphUtils.loadEdgesFromArgs(sc, args)

    val (graph, startTime) = GraphUtils.createGraphAndLog(rawEdges.filter(e => e.srcId != e.dstId))

    val (_, triangles) = new Brute_Force(spark).run(graph)

    val intersectCount = triangles.count { case (_, _, _, e1, e2, e3) =>
      (for {
        i1 <- GraphUtils.intersectIntervals(e1, e2)
        _ <- GraphUtils.intersectIntervals(i1, e3)
      } yield ()).isDefined
    }

    println(s"Total number of triangles: ${triangles.size / 3}")
    println(s"Formed triangles with edge intervals: ${intersectCount / 3}")
    println(s"Time taken to calculate triangle scores: ${System.currentTimeMillis() - startTime} milliseconds")
    GraphUtils.close(sc, spark)
  }
}