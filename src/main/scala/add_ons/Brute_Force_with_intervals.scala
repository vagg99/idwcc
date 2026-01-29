package add_ons

import org.apache.spark.graphx._
import org.apache.log4j.Logger
import scala.collection.mutable

class Brute_Force_with_intervals extends Serializable {

  def run(graph: Graph[Int, (Long, Long)]): VertexRDD[Double] = {
    runPreProcessed(graph)
  }

  private def runPreProcessed(graph: Graph[Int, (Long, Long)]): VertexRDD[Double] = {
    // Efficiently create a neighbor set along with the time intervals for each vertex
    val nbrSets: VertexRDD[mutable.HashMap[VertexId, (Long, Long)]] = {
      graph.aggregateMessages[mutable.HashMap[VertexId, (Long, Long)]](
        triplet => {
          val mapSrc = new mutable.HashMap[VertexId, (Long, Long)]()
          mapSrc.put(triplet.dstId, triplet.attr)
          triplet.sendToSrc(mapSrc)

          val mapDst = new mutable.HashMap[VertexId, (Long, Long)]()
          mapDst.put(triplet.srcId, triplet.attr)
          triplet.sendToDst(mapDst)
        },
        (a, b) => {
          a ++= b
          a
        },
        TripletFields.All
      )
    }

    // Join the graph with the neighbor sets
    val setGraph: Graph[mutable.HashMap[VertexId, (Long, Long)], (Long, Long)] = graph.outerJoinVertices(nbrSets) {
      (_, _, optSet) => optSet.getOrElse(new mutable.HashMap[VertexId, (Long, Long)]())
    }

    def intersectIntervals(interval1: (Long, Long), interval2: (Long, Long)): Option[(Long, Long)] = {
      val start = Math.max(interval1._1, interval2._1)
      val end = Math.min(interval1._2, interval2._2)
      if (start <= end) Some((start, end)) else None
    }

    def edgeFunc(ctx: EdgeContext[mutable.HashMap[VertexId, (Long, Long)], (Long, Long), Double]): Unit = {
      val (smallSet, largeSet) = if (ctx.srcAttr.size <= ctx.dstAttr.size) {
        (ctx.srcAttr, ctx.dstAttr)
      } else {
        (ctx.dstAttr, ctx.srcAttr)
      }

      var score: Double = 0.0
      val srcDstOpt = ctx.srcAttr.get(ctx.dstId)
      for (commonVertex <- smallSet.keys) {
        val dstCommonOpt = ctx.dstAttr.get(commonVertex)
        if (dstCommonOpt.isDefined) {
          val interSrcDstOpt = intersectIntervals(srcDstOpt.get, dstCommonOpt.get)
          val srcCommonOpt = ctx.srcAttr.get(commonVertex)
          if (srcCommonOpt.isDefined && interSrcDstOpt.isDefined) {
            val interSrcDstOpt2 = intersectIntervals(srcCommonOpt.get, interSrcDstOpt.get)
            if (interSrcDstOpt2.isDefined) {
              score += 1
            }
          }
        }
      }

      ctx.sendToSrc(score / 2)
      ctx.sendToDst(score / 2)
    }

    val scores: VertexRDD[Double] = setGraph.aggregateMessages(ctx => edgeFunc(ctx), _ + _)
    scores
  }
}

object Brute_Force_with_intervals {
  def main(args: Array[String]): Unit = {

    val (sc, spark) = GraphUtils.setupSpark("Brute_Force_with_intervals")

    // LOAD DATA
    // If args exist, use args(0), otherwise GraphUtils uses the default automatically
    val edges = if (args.length > 0) {
      GraphUtils.loadEdges(sc, args(0))
    } else {
      GraphUtils.loadEdges(sc)
    }

    val graph = Graph.fromEdges(edges, defaultValue = 1)

    Logger.getRootLogger.warn("graph is loaded!!")
    // Uncomment the next line if you want to verify counts (might take extra time on large graphs)
    // Logger.getRootLogger.warn(s"vertices: ${graph.vertices.count}, edges: ${graph.edges.count}")

    val triangleScorer = new Brute_Force_with_intervals()

    Logger.getRootLogger.warn("Phase: Preprocessing - Counting Triangles")

    val startTime = System.currentTimeMillis()

    val scores = triangleScorer.run(graph)

    // Aggregate the total number of triangles
    val totalTriangles = scores.map(_._2).reduce(_ + _) / 3
    println(s"Total number of triangles:")
    println(totalTriangles)

    // Record the end time
    val endTime = System.currentTimeMillis()

    // Calculate the elapsed time
    println(s"Time taken to calculate triangle scores: ${endTime - startTime} milliseconds")

    sc.stop()
    spark.stop()
  }
}