package tennis_analysis.I.finalatp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.util.ProcfsMetricsGetter").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ATP Tour Analysis")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val graph = GraphOperations.createGraph(sc)
    
    GraphOperations.displayGraphInfo(graph)
    GraphOperations.generateGraphVisualization(graph)
    
    Analysis.runAllAnalysis(graph)

    spark.stop()
  }
}
