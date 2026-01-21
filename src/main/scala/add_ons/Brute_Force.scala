package add_ons

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable.ListBuffer

// Define accumulator for triangles
class TriangleAccumulator extends AccumulatorV2[(VertexId, VertexId, VertexId, (Long, Long), (Long, Long), (Long, Long)), List[(VertexId, VertexId, VertexId, (Long, Long), (Long, Long), (Long, Long))]] {
  private val triangles = ListBuffer[(VertexId, VertexId, VertexId, (Long, Long), (Long, Long), (Long, Long))]()

  override def isZero: Boolean = triangles.isEmpty

  override def copy(): TriangleAccumulator = {
    val newAcc = new TriangleAccumulator()
    newAcc.triangles ++= triangles
    newAcc
  }

  override def reset(): Unit = triangles.clear()

  override def add(v: (VertexId, VertexId, VertexId, (Long, Long), (Long, Long), (Long, Long))): Unit = {
    triangles += v
  }

  override def merge(other: AccumulatorV2[(VertexId, VertexId, VertexId, (Long, Long), (Long, Long), (Long, Long)), List[(VertexId, VertexId, VertexId, (Long, Long), (Long, Long), (Long, Long))]]): Unit = {
    triangles ++= other.value
  }

  override def value: List[(VertexId, VertexId, VertexId, (Long, Long), (Long, Long), (Long, Long))] = triangles.toList
}

class Brute_Force(spark: SparkSession) extends Serializable {

  // Method to run the main computation
  def run(graph: Graph[Int, (Long, Long)]): (VertexRDD[Double], List[(VertexId, VertexId, VertexId, (Long, Long), (Long, Long), (Long, Long))]) = {
    runPreProcessed(graph)
  }

  private def runPreProcessed(graph: Graph[Int, (Long, Long)]): (VertexRDD[Double], List[(VertexId, VertexId, VertexId, (Long, Long), (Long, Long), (Long, Long))]) = {
    // Efficiently create a neighbor set along with the time intervals for each vertex
    val nbrSets: VertexRDD[Map[VertexId, (Long, Long)]] = {
      graph.aggregateMessages[Map[VertexId, (Long, Long)]](
        triplet => {
          triplet.sendToSrc(Map(triplet.dstId -> triplet.attr))
          triplet.sendToDst(Map(triplet.srcId -> triplet.attr))
        },
        (a, b) => a ++ b
      )
    }

    // Join the graph with the neighbor sets
    val setGraph: Graph[Map[VertexId, (Long, Long)], (Long, Long)] = graph.outerJoinVertices(nbrSets) {
      (_, _, optSet) => optSet.getOrElse(Map.empty)
    }

    // Initialize accumulator for triangles
    val trianglesAccumulator = new TriangleAccumulator()
    spark.sparkContext.register(trianglesAccumulator, "TrianglesAccumulator")

    def edgeFunc(ctx: EdgeContext[Map[VertexId, (Long, Long)], (Long, Long), Double]): Unit = {
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
          score += 1

          // Collect triangles with their edge intervals
          val triangle = List(ctx.srcId, commonVertex, ctx.dstId).sorted match {
            case List(a, b, c) => (a, b, c)
          }
          val edge1Interval = ctx.srcAttr(commonVertex)
          val edge2Interval = ctx.dstAttr(commonVertex)
          val edge3Interval = ctx.srcAttr(ctx.dstId)

          trianglesAccumulator.add((triangle._1, triangle._2, triangle._3, edge1Interval, edge2Interval, edge3Interval))
        }
      }

      ctx.sendToSrc(score / 2)
      ctx.sendToDst(score / 2)
    }

    val scores: VertexRDD[Double] = setGraph.aggregateMessages(ctx => edgeFunc(ctx), _ + _)

    // Force evaluation of scores RDD with an action like collect
    scores.collect()

    // Return scores and collected triangles to the driver
    (scores, trianglesAccumulator.value)
  }
}

object Brute_Force {
  def main(args: Array[String]): Unit = {
    // Set Log4j level to WARN
    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getRootLogger.warn("Getting context!!")

    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("BruteForceTriangles")
      .master("local[*]")
      .getOrCreate()

    // Read the file and skip header lines
    val dataPath = "data/amazon_generated_intervals.txt"
    val lines = spark.sparkContext.textFile(dataPath).filter(!_.startsWith("#")) // Skip header lines

    // Define the schema for edges
    val edgesRDD = lines.map { line =>
      val parts = line.split("\t")
      Edge(parts(0).toLong, parts(1).toLong, (parts(2).toLong, parts(3).toLong))
    }.filter(edge => edge.srcId != edge.dstId) // Filter out self-loops

    // Create RDD for vertices
    val verticesRDD = edgesRDD.flatMap(edge => Seq((edge.srcId, 1), (edge.dstId, 1))).distinct()

    // Create GraphX graph from vertices and edges RDDs
    val graph: Graph[Int, (Long, Long)] = Graph(verticesRDD, edgesRDD)

    Logger.getRootLogger.warn("Graph is loaded!!")
    Logger.getRootLogger.warn(s"Vertices: ${graph.vertices.count}, Edges: ${graph.edges.count}")

    // Create an instance of Brute_Force
    val triangleScorer = new Brute_Force(spark)

    Logger.getRootLogger.warn("Phase: Preprocessing - Counting Triangles")

    // Record the start time
    val startTime = System.currentTimeMillis()

    // Run the algorithm
    val (scores, triangles) = triangleScorer.run(graph)

    // Aggregate the total number of triangles
    val totalTriangles = triangles.size/3
    println(s"Total number of triangles: $totalTriangles")
    var kostas: Int = 0
    // Print formed triangles with edge intervals
    println("Formed triangles with edge intervals:")
    //print(triangles.size/3)
    triangles.foreach { case (v1, v2, v3, e1, e2, e3) =>
      // Debug output for triangles


      // Check for edge intersection and print intervals
      if (intersectIntervals(e1, e2) && intersectIntervals(e1, e3) && intersectIntervals(e2, e3)) {
        kostas += 1
      }
    }
    val endTime = System.currentTimeMillis()
    println(kostas/3)

    // Calculate the elapsed time
    val elapsedTime = endTime - startTime
    println(s"Time taken to calculate triangle scores: $elapsedTime milliseconds")

    // Stop Spark session
    spark.stop()
  }

  // Function to check if two intervals intersect
  def intersectIntervals(interval1: (Long, Long), interval2: (Long, Long)): Boolean = {
    val start = Math.max(interval1._1, interval2._1)
    val end = Math.min(interval1._2, interval2._2)
    start <= end
  }
}
