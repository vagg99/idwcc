package add_ons

import org.apache.spark.graphx._
import org.apache.log4j.Logger

class TriangleCount_GraphFiltering extends Serializable {

  def run(queryTimeInterval: (Long, Long), graph: Graph[Int, (Long, Long)]): VertexRDD[Double] = {
    runPreProcessed(graph, queryTimeInterval)
  }

  private def runPreProcessed(graph: Graph[Int, (Long, Long)], queryTimeInterval: (Long, Long)): VertexRDD[Double] = {
    // Create a neighbor set along with the time intervals for each vertex using collectEdges
    val nbrSets: VertexRDD[Map[VertexId, (Long, Long)]] = {
      // Collecting both incoming and outgoing edges to ensure all neighbors are captured
      graph.collectEdges(EdgeDirection.Either).mapValues { edges =>
        // Iterate over each edge to collect neighbors
        edges.foldLeft(Map.empty[VertexId, (Long, Long)]) { (neighborMap, e) =>
          if (e.srcId != e.dstId) {
            // Add the edge to the neighbor map of the source vertex
            neighborMap + (e.dstId -> e.attr) + (e.srcId -> e.attr)
          } else {
            // If it's a self-loop, just add it once to the neighbor map
            neighborMap + (e.srcId -> e.attr)
          }
        }
      }
    }

    val setGraph: Graph[Map[VertexId, (Long, Long)], (Long, Long)] = graph.outerJoinVertices(nbrSets) {
      (_, _, optSet) => optSet.getOrElse(Map.empty)
    }

    def intersectIntervals(edge1TimeInterval: (Long, Long), queryTimeInterval: (Long, Long)): Option[(Long, Long)] = {
      val start = Math.max(edge1TimeInterval._1, queryTimeInterval._1)
      val end = Math.min(edge1TimeInterval._2, queryTimeInterval._2)
      if (start <= end) {
        Some((start, end))
      } else {
        None
      }
    }

    def edgeFunc(ctx: EdgeContext[Map[VertexId, (Long, Long)], (Long, Long), Double], queryTimeInterval: (Long, Long)): Unit = {
      val (smallSet, largeSet) = if (ctx.srcAttr.size <= ctx.dstAttr.size) {
        (ctx.srcAttr, ctx.dstAttr)
      } else {
        (ctx.dstAttr, ctx.srcAttr)
      }
      val iter = smallSet.keys.iterator
      var score: Double = 0.0
      while (iter.hasNext) {
        val commonVertex = iter.next()
        if (commonVertex != ctx.srcId && commonVertex != ctx.dstId && largeSet.contains(commonVertex)) {

          // Calculate the intersections step-by-step and stop if any are None
          val interTimeIntervalOpt = intersectIntervals(ctx.srcAttr(commonVertex), ctx.dstAttr(commonVertex))
          interTimeIntervalOpt match {
            case Some(interTimeInterval) =>
              val intersectedTimeIntervalOpt = intersectIntervals(interTimeInterval, ctx.srcAttr(ctx.dstId))
              intersectedTimeIntervalOpt match {
                case Some(intersectedTimeInterval) =>
                  val intersectTimeIntervalOpt = intersectIntervals(intersectedTimeInterval, queryTimeInterval)
                  intersectTimeIntervalOpt match {
                    case Some(intersectTimeInterval) =>
                      val intersectLength = intersectTimeInterval._2 - intersectTimeInterval._1 + 1
                      val queryLength = queryTimeInterval._2 - queryTimeInterval._1 + 1
                      // val triangleScore = intersectLength.toDouble / queryLength
                      // (Note: Unused variable logic from original script kept as comment)
                      score += 1
                    case None => // No valid final intersect time interval
                  }
                case None => // No valid intersected time interval
              }
            case None => // No valid inter time interval
          }
        }
      }

      ctx.sendToSrc(score / 2)
      ctx.sendToDst(score / 2)
    }

    val scores: VertexRDD[Double] = setGraph.aggregateMessages(ctx => edgeFunc(ctx, queryTimeInterval), _ + _)
    scores
  }
}

object TriangleCount_GraphFiltering {
  def main(args: Array[String]): Unit = {

    // --- 1. SETUP ---
    val (sc, spark) = GraphUtils.setupSpark("TriangleCount_GraphFiltering")

    // --- 2. CONFIGURATION ---
    // Use command line argument for file path if provided, otherwise default to Amazon
    val inputPath = if (args.length > 0) args(0) else "data/amazon_generated_intervals.txt"
    val queryTimeInterval: (Long, Long) = (2, 2)

    // --- 3. LOAD DATA ---
    Logger.getRootLogger.warn(s"Loading edges from $inputPath...")
    val edges = GraphUtils.loadEdges(sc, inputPath).cache()
    Logger.getRootLogger.warn("edges loaded!!")

    // Record the start time
    val startTime = System.currentTimeMillis()

    // --- 4. EXECUTE ALGORITHM ---
    // Filter the edges based on the query time interval
    val filteredEdges = edges.filter { edge =>
      val (attr1, attr2) = edge.attr
      val start = Math.max(attr1, queryTimeInterval._1)
      val end = Math.min(attr2, queryTimeInterval._2)
      start <= end
    }

    // Create the subgraph directly from the filtered edges
    val subgraph = Graph.fromEdges(filteredEdges, defaultValue = 1)

    // Create an instance of the class and run
    val triangleScorer = new TriangleCount_GraphFiltering()
    val scores = triangleScorer.run(queryTimeInterval, subgraph)

    // Aggregate the total number of triangles
    val totalTriangles = scores.map(_._2).reduce(_ + _) / 3
    println(s"Total number of triangles: $totalTriangles")

    // Record the end time
    val endTime = System.currentTimeMillis()

    // Calculate the elapsed time
    val elapsedTime = endTime - startTime
    println(s"Time taken to calculate triangle scores: $elapsedTime milliseconds")

    // --- 5. TEARDOWN ---
    sc.stop()
    spark.stop()
  }
}