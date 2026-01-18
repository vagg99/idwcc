package add_ons

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
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

    val conf = new SparkConf().setAppName("TriangleScoringWithTimeIntervals").setMaster("local[*]")
    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getRootLogger.warn("Getting context!!")
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

    // Create the graph from the edges RDD
    val graph: Graph[Int, (Long, Long)] = Graph.fromEdges(edges, defaultValue = 1)

    Logger.getRootLogger.warn("graph is loaded!!")
    Logger.getRootLogger.warn(s"vertices: ${graph.vertices.count}, edges: ${graph.edges.count}")

    // Create an instance of TriangleScoringWithTimeIntervals
    val triangleScorer = new Brute_Force_with_intervals()

    Logger.getRootLogger.warn("Phase: Preprocessing - Counting Triangles")

    // Record the start time
    val startTime = System.currentTimeMillis()

    // Run the algorithm
    val scores = triangleScorer.run(graph)

    // Aggregate the total number of triangles
    val totalTriangles = scores.map(_._2).reduce(_ + _) / 3
    println(s"Total number of triangles:")
    println(totalTriangles)

    // Record the end time
    val endTime = System.currentTimeMillis()

    // Calculate the elapsed time
    val elapsedTime = endTime - startTime
    println(s"Time taken to calculate triangle scores: $elapsedTime milliseconds")

    sc.stop()
    spark.stop()
  }
}

