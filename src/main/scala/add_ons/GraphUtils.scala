package add_ons

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable

object GraphUtils {

  /**
   * Centralized query time window used by all algorithms.
   *
   * The 1st number marks the window's start timestamp (inclusive).
   *
   * The 2nd number marks the window's end timestamp (inclusive).
   *
   * @example
   *  val QUERY_TIME_INTERVAL: (Long, Long) = (1L, 7L)
   *
   *  Valid discrete time moments:
   *
   *  [1, 2, 3, 4, 5, 6, 7]
   */
  val QUERY_TIME_INTERVAL: (Long, Long) = (2L, 6L)

  def queryLen: Long = QUERY_TIME_INTERVAL._2 - QUERY_TIME_INTERVAL._1 + 1

  private val DATASET_NAME = "orkut"
  private val DefaultDataset = s"data/${DATASET_NAME}_generated_intervals.txt"

  type TriangleMetadata = (VertexId, VertexId, VertexId, (Long, Long), (Long, Long), (Long, Long))

  private val logger = Logger.getRootLogger

  def log(msg: String): Unit = logger.warn(msg)

  class TriangleAccumulator extends AccumulatorV2[TriangleMetadata, List[TriangleMetadata]] {
    private val collectedTriangles = mutable.ListBuffer[TriangleMetadata]()

    override def isZero: Boolean = collectedTriangles.isEmpty

    override def copy(): TriangleAccumulator = {
      val newAcc = new TriangleAccumulator()
      newAcc.collectedTriangles ++= this.collectedTriangles
      newAcc
    }

    override def reset(): Unit = collectedTriangles.clear()

    override def add(v: TriangleMetadata): Unit = collectedTriangles += v

    override def merge(other: AccumulatorV2[TriangleMetadata, List[TriangleMetadata]]): Unit =
      collectedTriangles ++= other.value

    override def value: List[TriangleMetadata] = collectedTriangles.toList
  }

  def setupSpark(appName: String): (SparkContext, SparkSession) = {
    val conf = new SparkConf().setAppName(appName).setMaster("local[*]")
    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.ERROR)
    log("Getting context!!")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    log("We have context!!")
    (sc, spark)
  }

  def loadEdgesFromArgs(sc: SparkContext, args: Array[String]): RDD[Edge[(Long, Long)]] = {
    val edges = if (args.length > 0) loadEdges(sc, args(0)) else loadEdges(sc)
    log("edges loaded!!")
    edges
  }

  private def loadEdges(sc: SparkContext, path: String = DefaultDataset): RDD[Edge[(Long, Long)]] = {
    sc.textFile(path)
      .filter(line => !line.startsWith("#"))
      .map { line =>
        val fields = line.split("\t")
        Edge(fields(0).toLong, fields(1).toLong, (fields(2).toLong, fields(3).toLong))
      }
  }

  def createGraphAndLog(edges: RDD[Edge[(Long, Long)]]): (Graph[Int, (Long, Long)], Long) = {
    val graph = Graph.fromEdges(edges, defaultValue = 1)
    log("graph is loaded!!")
    log(s"vertices: ${graph.vertices.count()}, edges: ${graph.edges.count()}")
    (graph, System.currentTimeMillis())
  }

  def aggregateAndReport(scores: VertexRDD[Double], startTime: Long, sc: SparkContext, spark: SparkSession): Unit = {
    val total = if (scores.isEmpty()) 0.0 else scores.map(_._2).sum() / 3.0
    reportResults(total, startTime, sc, spark)
  }

  private def reportResults(totalTriangles: Double, startTime: Long, sc: SparkContext, spark: SparkSession): Unit = {
    println(s"Total number of triangles: $totalTriangles")
    println(s"Time taken to calculate triangle scores: ${System.currentTimeMillis() - startTime} milliseconds")
    close(sc, spark)
  }

  def intersectIntervals(i1: (Long, Long), i2: (Long, Long)): Option[(Long, Long)] = {
    val start = Math.max(i1._1, i2._1)
    val end = Math.min(i1._2, i2._2)
    if (start <= end) Some((start, end)) else None
  }

  def close(sc: SparkContext, spark: SparkSession): Unit = {
    sc.stop()
    spark.stop()
  }

  private def getNeighborSets(graph: Graph[_, (Long, Long)]): VertexRDD[mutable.HashMap[VertexId, (Long, Long)]] = {
    graph.aggregateMessages[mutable.HashMap[VertexId, (Long, Long)]](
      triplet => {
        triplet.sendToSrc(mutable.HashMap(triplet.dstId -> triplet.attr))
        triplet.sendToDst(mutable.HashMap(triplet.srcId -> triplet.attr))
      },
      (a, b) => {
        a ++= b
        a
      },
      TripletFields.All
    )
  }

  def orderSets(
                 setA: mutable.Map[VertexId, (Long, Long)],
                 setB: mutable.Map[VertexId, (Long, Long)]
               ): (mutable.Map[VertexId, (Long, Long)], mutable.Map[VertexId, (Long, Long)]) = {
    if (setA.size <= setB.size) (setA, setB) else (setB, setA)
  }

  def prepareSetGraph(graph: Graph[Int, (Long, Long)]): Graph[mutable.HashMap[VertexId, (Long, Long)], (Long, Long)] = {
    val nbrSets = getNeighborSets(graph)
    graph.outerJoinVertices(nbrSets) { (_, _, optSet) =>
      optSet.getOrElse(mutable.HashMap.empty[VertexId, (Long, Long)])
    }
  }

  // Default: uses the centralized QUERY_TIME_INTERVAL.
  // If you ever need to run "no query interval", call executeTriangleCounting(graph, None).
  def executeTriangleCounting(
                               graph: Graph[Int, (Long, Long)],
                               queryInterval: Option[(Long, Long)] = Some(QUERY_TIME_INTERVAL)
                             ): VertexRDD[Double] = {
    log("Phase: Preprocessing - Counting Triangles")
    val setGraph = prepareSetGraph(graph)
    computeTriangleScores(setGraph, queryInterval)
  }

  def computeTriangleScores(
                             setGraph: Graph[mutable.HashMap[VertexId, (Long, Long)], (Long, Long)],
                             queryInterval: Option[(Long, Long)] = Some(QUERY_TIME_INTERVAL)
                           ): VertexRDD[Double] = {
    setGraph.aggregateMessages[Double](
      ctx => {
        val (smallSet, largeSet) = orderSets(ctx.srcAttr, ctx.dstAttr)
        var score: Double = 0.0
        val srcDstTime = ctx.srcAttr(ctx.dstId)

        for (v <- smallSet.keys if v != ctx.srcId && v != ctx.dstId && largeSet.contains(v)) {
          val validTriangle = for {
            i1 <- intersectIntervals(srcDstTime, ctx.dstAttr(v))
            i2 <- intersectIntervals(i1, ctx.srcAttr(v))
            _ <- queryInterval match {
              case Some(qi) => intersectIntervals(i2, qi)
              case None => Some(i2)
            }
          } yield ()

          if (validTriangle.isDefined) score += 1.0
        }

        ctx.sendToSrc(score / 2.0)
        ctx.sendToDst(score / 2.0)
      },
      (a, b) => a + b
    )
  }
}
