package add_ons

import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

class Brute_Force(spark: SparkSession) extends Serializable {

  def run(graph: Graph[Int, (Long, Long)]): (VertexRDD[Double], List[GraphUtils.TriangleMetadata]) = {
    // Uses the shared helper to avoid duplication warnings
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
    val rawEdges = if (args.length > 0) GraphUtils.loadEdges(sc, args(0)) else GraphUtils.loadEdges(sc)
    val graph = Graph.fromEdges(rawEdges.filter(e => e.srcId != e.dstId), defaultValue = 1)

    val startTime = System.currentTimeMillis()
    val (_, triangles) = new Brute_Force(spark).run(graph)

    val intersectCount = triangles.count { case (_, _, _, e1, e2, e3) =>
      (for {
        i1 <- GraphUtils.intersectIntervals(e1, e2)
        _  <- GraphUtils.intersectIntervals(i1, e3)
      } yield ()).isDefined
    }

    println(s"Total triangles: ${triangles.size / 3} | Intersecting: ${intersectCount / 3}")
    println(s"Time taken: ${System.currentTimeMillis() - startTime} ms")
    GraphUtils.close(sc, spark)
  }
}