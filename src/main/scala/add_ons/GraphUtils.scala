package add_ons

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}

object GraphUtils {

  /**
   * Initializes SparkContext and SparkSession with standard configuration.
   * Reduces log noise automatically.
   */
  def setupSpark(appName: String): (SparkContext, SparkSession) = {
    val conf = new SparkConf().setAppName(appName).setMaster("local[*]")

    // Set logging to reduce noise
    Logger.getRootLogger.setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    Logger.getRootLogger.warn(s"Context initialized for $appName")
    (sc, spark)
  }

  /**
   * Loads edges from a text file (tab-separated).
   * Format: srcId \t dstId \t attr1 \t attr2
   */
  def loadEdges(sc: SparkContext, path: String): RDD[Edge[(Long, Long)]] = {
    sc.textFile(path)
      .filter(line => !line.startsWith("#")) // Skip header lines
      .map { line =>
        val fields = line.split("\t")
        val srcId = fields(0).toLong
        val dstId = fields(1).toLong
        val attr1 = fields(2).toLong
        val attr2 = fields(3).toLong
        Edge(srcId, dstId, (attr1, attr2))
      }
  }
}