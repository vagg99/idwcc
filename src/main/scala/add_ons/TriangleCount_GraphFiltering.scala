package add_ons

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

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
                //val queryLength = queryTimeInterval._2 - queryTimeInterval._1 + 1
                //val overlapRequired = (overlapPercentage / 100.0) * queryLength

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
                            val triangleScore = intersectLength.toDouble / queryLength
                            score += 1
                          case None => // No valid final intersect time interval, skip to the next triangle
                        }
                      case None => // No valid intersected time interval, skip to the next triangle
                    }
                  case None => // No valid inter time interval, skip to the next triangle
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

    val conf = new SparkConf().setAppName("TriangleCount_GraphFiltering").setMaster("local[*]")
    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getRootLogger.warn("Getting context!!")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    Logger.getRootLogger.warn("We have context!!")

    // Read the file and skip header lines
    val lines = sc.textFile("data/amazon_generated_intervals.txt")
    val edges = lines
      .filter(line => !line.startsWith("#")) // Skip header lines
      .map { line =>
        val fields = line.split("\t")
        val srcId = fields(0).toLong
        val dstId = fields(1).toLong
        val attr1 = fields(2).toLong
        val attr2 = fields(3).toLong
        Edge(srcId, dstId, (attr1, attr2))
      }.cache

    Logger.getRootLogger.warn("edges loaded!!")

    // Define the time interval
    val queryTimeInterval: (Long, Long) = (2, 2)

    // Record the start time
    val startTime = System.currentTimeMillis()

    // Filter the edges based on the query time interval
    val filteredEdges = edges.filter { edge =>
      val (attr1, attr2) = edge.attr
      val start = Math.max(attr1, queryTimeInterval._1)
      val end = Math.min(attr2, queryTimeInterval._2)
      start <= end
    }


    // Create the subgraph directly from the filtered edges
    val subgraph = Graph.fromEdges(filteredEdges, defaultValue = 1)

    //Logger.getRootLogger.warn(s"filtered vertices: ${subgraph.vertices.count}, filtered edges: ${subgraph.edges.count}")


    // Create an instance of TriangleScoringWithTimeIntervals
    val triangleScorer = new TriangleCount_GraphFiltering()

    //Logger.getRootLogger.warn("Phase: Preprocessing - Counting Triangles")
    // Run the algorithm on the subgraph
    val scores = triangleScorer.run(queryTimeInterval, subgraph)

    // Aggregate the total number of triangles
    val totalTriangles = scores.map(_._2).reduce(_ + _) / 3
    println(s"Total number of triangles: $totalTriangles")

    // Record the end time
    val endTime = System.currentTimeMillis()

    // Calculate the elapsed time
    val elapsedTime = endTime - startTime
    println(s"Time taken to calculate triangle scores: $elapsedTime milliseconds")


    sc.stop()
    spark.stop()
  }
}


