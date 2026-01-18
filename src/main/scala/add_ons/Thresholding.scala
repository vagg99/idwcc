package add_ons

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

class Thresholding extends Serializable {
  def run(queryTimeInterval: (Long, Long), minTimeInstancesRequired: Long, graph: Graph[Int, (Long, Long)]): VertexRDD[Double] = {
    // Print the edges
    runPreProcessed(graph, queryTimeInterval, minTimeInstancesRequired)
  }

  private def runPreProcessed(graph: Graph[Int, (Long, Long)], queryTimeInterval: (Long, Long), minTimeInstancesRequired: Long): VertexRDD[Double] = {
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

    def intersectIntervals(edge1TimeInterval: (Long, Long), queryTimeInterval: (Long, Long)): Option[(Long, Long)] = {
      val start = Math.max(edge1TimeInterval._1, queryTimeInterval._1)
      val end = Math.min(edge1TimeInterval._2, queryTimeInterval._2)
      if (start <= end) Some((start, end)) else None
    }

    def edgeFunc(ctx: EdgeContext[mutable.HashMap[VertexId, (Long, Long)], (Long, Long), Double], queryTimeInterval: (Long, Long)): Unit = {
      val (smallSet, largeSet) = if (ctx.srcAttr.size <= ctx.dstAttr.size) {
        (ctx.srcAttr, ctx.dstAttr)
      } else {
        (ctx.dstAttr, ctx.srcAttr)
      }

      val iter = smallSet.keys.iterator
      var score: Double = 0.0
      val srcDstOpt = ctx.srcAttr.get(ctx.dstId)
      while (iter.hasNext) {
        val commonVertex = iter.next()
        val dstCommonOpt = ctx.dstAttr.get(commonVertex)
        if (dstCommonOpt.isDefined) {
          val interSrcDstOpt = intersectIntervals(srcDstOpt.get, dstCommonOpt.get)
          val srcCommonOpt = ctx.srcAttr.get(commonVertex)
          if (interSrcDstOpt.isDefined && srcCommonOpt.isDefined) {
            val interSrcDstOpt1 = intersectIntervals(interSrcDstOpt.get, srcCommonOpt.get)
            //println(interSrcDstOpt2)
            if (interSrcDstOpt1.isDefined) {
              score += 1

            }
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

object Thresholding {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Thresholding").setMaster("local[*]")
    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getRootLogger.warn("Getting context!!")
    // Create a Spark context
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    Logger.getRootLogger.warn("We have context!!")

    // Read the file and skip header lines
    val lines = sc.textFile("data/amazon777.txt")
    val edges = lines
      .filter(line => !line.startsWith("#")) // Skip header lines
      .map { line =>
        val fields = line.split("\t")
        val srcId = fields(0).toLong
        val dstId = fields(1).toLong
        val attr1 = fields(2).toLong
        val attr2 = fields(3).toLong
        Edge(srcId, dstId, (attr1, attr2))
      }

    // Record the start time for algorithm execution
    val algorithmStartTime = System.currentTimeMillis()

    // Define the time interval and overlap percentage
    val queryTimeInterval: (Long, Long) = (2,2)
    val overlapPercentage: Double = 0.0 // Example overlap percentage

    // Calculate the number of time instances in the query time interval
    val queryTimeIntervalLength = queryTimeInterval._2 - queryTimeInterval._1 + 1

    // Calculate the minimum number of time instances required based on the percentage
    val minTimeInstancesRequired = (overlapPercentage / 100.0 * queryTimeIntervalLength).toInt

    // Filter the edges based on the percentage of the time instances included in the intersection
    val filteredEdges = edges.flatMap { edge =>
      // Extract the edge's time interval
      val (attr1, attr2) = edge.attr

      // Calculate the intersection with the query time interval
      val start = Math.max(attr1, queryTimeInterval._1)
      val end = Math.min(attr2, queryTimeInterval._2)

      // Calculate the length of the intersection
      val edgeTimeIntervalLength = end - start + 1

      // Calculate the actual overlap percentage
      val overlapPercentageActual = (edgeTimeIntervalLength.toDouble / queryTimeIntervalLength) * 100.0

      // Ensure the overlap length is not zero and meets the required overlap percentage
      if (edgeTimeIntervalLength != 0 && overlapPercentageActual >= overlapPercentage) {
        // Create a new edge with the estimated time interval
        Some(edge.copy(attr = (start, end)))
      } else {
        None
      }
    }


    // Create the subgraph directly from the filtered edges
    val subgraph = Graph.fromEdges(filteredEdges, defaultValue = 1)

    // Print the edges of the subgraph
    //subgraph.edges.collect().foreach(println)

    // Create an instance of Thresholding
    val thresholding = new Thresholding()

    // Run the algorithm
    val scores = thresholding.run(queryTimeInterval, minTimeInstancesRequired, subgraph)

    // Aggregate the total number of triangles
    val totalTriangles = if (!scores.isEmpty()) {
      scores.map(_._2).reduce(_ + _) / 3
    } else {
      0.0
    }
    println(s"Total number of triangles: $totalTriangles")

    // Record the end time
    val algorithmEndTime = System.currentTimeMillis()

    // Calculate the elapsed time
    val elapsedTime = algorithmEndTime - algorithmStartTime
    println(s"Time taken to calculate triangle scores: $elapsedTime milliseconds")

    sc.stop()
    spark.stop()
  }
}

