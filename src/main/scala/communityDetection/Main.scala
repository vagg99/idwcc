package communityDetection

import org.graphframes._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import wcc.GraphFrameOps._

object Main {
  val FULLRUN = 1
  val STREAM = 2

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CommunityDetection")
    val filePath = "data/custom.txt"
    Logger.getLogger("org").setLevel(Level.ERROR)

    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[*]")
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getRootLogger.warn("Getting context!!")

    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val spark = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()

    Logger.getRootLogger.warn("We have context!!")
    Logger.getRootLogger.warn(s"Dataset: $filePath")

    val operation = FULLRUN

    operation match {
      case FULLRUN =>
        loadAndRegionalizeGraph(spark, filePath)

      case STREAM =>
        CSVGraph.testStream(spark, filePath)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def loadAndRegionalizeGraph(spark: SparkSession, filePath: String, edgeCount: Double = 0) = {
    val graph = if (edgeCount == 0) {
      CSVGraph.loadGraph(spark, filePath)
    } else {
      CSVGraph.loadSampleGraph(spark, filePath, edgeCount)
    }
    graph.cache()

    Logger.getRootLogger.warn("graph is loaded!!")
    Logger.getRootLogger.warn(s"vertices: ${graph.vertices.count}, edges: ${graph.edges.count}")

    val communityDF = graph.scalableCommunityDetection(spark)
    val regionalizedGraph = GraphFrame(graph.vertices.join(communityDF, "id"), graph.edges).cache()
    regionalizedGraph
  }
}