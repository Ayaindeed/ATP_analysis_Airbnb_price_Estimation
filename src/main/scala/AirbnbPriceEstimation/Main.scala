package AirbnbPriceEstimation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    // Initialisation SparkSession
    val spark = SparkSession.builder()
      .appName("Airbnb Price Estimation")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val csvPath = "Datasets/airbnb-data.csv"

    // Chargement et eda
    val dfInitial = DataProcessing.chargerDonnees(spark, csvPath)
    DataProcessing.eda(dfInitial)

    // Préparation des données
    val dfPrepare = DataProcessing.preparerDonnees(dfInitial)

    println("\n Schéma :")
    dfPrepare.printSchema()

    // Stats Prix après filtrage des outliers
    println("\n Stats Prix (après filtrage outliers 10-1000$):")
    dfPrepare.select(
      org.apache.spark.sql.functions.min(org.apache.spark.sql.functions.exp(org.apache.spark.sql.functions.col("label"))).alias("prix_min"),
      org.apache.spark.sql.functions.max(org.apache.spark.sql.functions.exp(org.apache.spark.sql.functions.col("label"))).alias("prix_max"),
      org.apache.spark.sql.functions.avg(org.apache.spark.sql.functions.exp(org.apache.spark.sql.functions.col("label"))).alias("prix_moyen"),
      org.apache.spark.sql.functions.stddev(org.apache.spark.sql.functions.exp(org.apache.spark.sql.functions.col("label"))).alias("prix_ecart_type")
    ).show()

    // Split train/test
    val Array(trainData, testData) = DataProcessing.splitData(dfPrepare)

    // Construction pipeline ML
    val pipelineRF = MLPipeline.pipelineML("rf")

    // Grille de paramètres
    val paramGridRF = MLPipeline.ParamGrid(pipelineRF, "rf")

    // Cross Validation et training
    println("\n Random Forest")
    val cvModelRF = MLTraining.model_Training(pipelineRF, paramGridRF, trainData)
    MLEvaluation.model_Eval(cvModelRF, testData, "Random Forest")

    spark.stop()
  }
}
