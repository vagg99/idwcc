/**
 *
 * This object is used to handle the advanced case of Historical Graphs.
 * Such graphs are used to simulate the evolution of a graph over time.
 * The main idea is to have a graph that is updated with new edges over time.
 * The graph is then used to calculate the WCC of the graph at each time step.
 * The WCC is then used to calculate the number of connected components in the graph.
 * Each connected component has a different amount of Countribution to the WCC.
 * The new idea implemented here is the addition of two new columns to the graph.
 * T_start: The time step at which the edge was added to the graph.
 * T_finish: The time step at which the edge was removed from the graph.
 * Likewise, the graph is updated with new edges at each time step. (either added or removed)
 *
 */

package wcc

import org.apache.spark.graphx._

object HistoricalDWCC {
  def main(args: Array[String]): Unit = {
  // simply check if the code compiles with Hello world
    println("Hello, world!")
  }




}
