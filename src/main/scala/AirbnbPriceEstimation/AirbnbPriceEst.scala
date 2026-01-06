package AirbnbPriceEstimation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{Imputer, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AirbnbPriceEst {

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
    val dfInitial = chargerDonnees(spark, csvPath)
    eda(dfInitial)

    // Préparation des données
    val dfPrepare = preparerDonnees(dfInitial)

    println("\n Schéma :")
    dfPrepare.printSchema()
    // Split train/test
    val Array(trainData, testData) = splitData(dfPrepare)

    // Construction pipeline ML
    val pipelineRF = pipelineML("rf")

    // Grille de paramètres
    val paramGridRF = ParamGrid(pipelineRF, "rf")

    // Cross Validation et training
    println("\n Random Forest")
    val cvModelRF = model_Training(pipelineRF, paramGridRF, trainData)
    model_Eval(cvModelRF, testData, "Random Forest")

    spark.stop()
  }

  // Fonction: Chargement du CSV
  def chargerDonnees(spark: SparkSession, path: String): org.apache.spark.sql.DataFrame = {
    println("Chargement des données")
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

    println(s"Nbr de lignes: ${df.count()}")
    println(s"Nbr de colonnes: ${df.columns.length}")
    df
  }

  // eda_
  def eda(df: org.apache.spark.sql.DataFrame): Unit = {
    println("\n EDA")

    // Schéma
    println("\nSchéma Initial")
    df.printSchema()

    println("\nAperçu des Données")
    df.show(5, truncate = false)

    // Stats descriptives
    println("\nStats Descriptives")
    df.describe().show()

    // Infos sur les colonnes
    println("\nTypes de Données")
    df.dtypes.foreach { case (colName, colType) =>
      println(s"$colName: $colType")
    }

    // Valeurs nulles par colonne
    println("\n Val nulles par col")
    df.columns.foreach { col =>
      val nullCount = df.filter(df(col).isNull || df(col) === "").count()
      println(s"$col: $nullCount")
    }

    // Distribution des types de chambres
    println("\n Dist room_type")
    df.groupBy("room_type").count().orderBy(desc("count")).show()

    // Distribution par quartier
    println("\nTop 10 Quartiers")
    df.groupBy("neighbourhood").count().orderBy(desc("count")).show(10)

    // Statistiques prix
    println("\n Stats Prix")
    df.select(
      min("price").alias("prix_min"),
      max("price").alias("prix_max"),
      avg("price").alias("prix_moyen"),
      stddev("price").alias("prix_ecart_type")
    ).show()
  }

  // Fonction: Préparation complète des données
  def preparerDonnees(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    println("\n Prép des données")

    // Concatenation host_id et id
    val df1 = df.withColumn("id", concat_ws("_", col("host_id"), col("id")))

    // Trans number_of_reviews en int
    val df2 = df1.withColumn("number_of_reviews", col("number_of_reviews").cast(IntegerType))

    // Trans reviews_per_month en double
    val df3 = df2.withColumn("reviews_per_month", col("reviews_per_month").cast(DoubleType))

    // Trans price en double (nettoyage si caractères spéciaux)
    val df4 = df3.withColumn("price",
      regexp_replace(col("price"), "[^0-9.]", "").cast(DoubleType))

    // Filtrer les prix <= 0 avant log transformation
    val df4b = df4.filter(col("price") > 0)

    // Appliquer log transformation au prix pour améliorer la distribution
    val df5 = df4b.withColumn("label", log(col("price")))
      .drop("price")

    // Cast numeric columns to DoubleType
    val df5b = df5
      .withColumn("latitude", col("latitude").cast(DoubleType))
      .withColumn("longitude", col("longitude").cast(DoubleType))
      .withColumn("minimum_nights", col("minimum_nights").cast(DoubleType))
      .withColumn("calculated_host_listings_count", col("calculated_host_listings_count").cast(DoubleType))
      .withColumn("availability_365", col("availability_365").cast(DoubleType))

    // Feature engineering: créer features additionnelles
    val df5c = df5b
      .withColumn("reviews_per_availability", 
        when(col("availability_365") > 0, col("number_of_reviews") / col("availability_365")).otherwise(0))
      .withColumn("has_reviews", when(col("number_of_reviews") > 0, 1.0).otherwise(0.0))
      .withColumn("log_minimum_nights", log1p(col("minimum_nights")))
      .withColumn("log_availability", log1p(col("availability_365")))

    // Supp col  host_id et neighbourhood_group
    val df6 = df5c.drop("host_id", "neighbourhood_group")

    // Traitement val nulles neighbourhood & room_type
    val dfFinal = df6.filter(
      col("neighbourhood").isNotNull && col("neighbourhood") =!= "" && col("room_type").isNotNull && col("room_type") =!= ""
    )
    println(s"\nNbr de lignes après préparation: ${dfFinal.count()}")

    dfFinal
  }
  // Fonction: Split données train/test (80/20)
  def splitData(df: org.apache.spark.sql.DataFrame): Array[org.apache.spark.sql.DataFrame] = {
    println("\n Split données 80/20")
    val Array(train, test) = df.randomSplit(Array(0.8, 0.2), seed = 42)
    println(s"Train: ${train.count()} lignes")
    println(s"Test: ${test.count()} lignes")
    Array(train, test)
  }

  // Construction du pipeline ML
  def pipelineML(modelType: String): Pipeline = {
    println(s"\n Construction pipeline ML ($modelType)")

    // StringIndexer pour colonnes catégorielles
    val neighbourhoodIndexer = new StringIndexer()
      .setInputCol("neighbourhood")
      .setOutputCol("neighbourhood_index")
      .setHandleInvalid("keep")

    val roomTypeIndexer = new StringIndexer()
      .setInputCol("room_type")
      .setOutputCol("room_type_index")
      .setHandleInvalid("keep")

    // Imputer pour valeurs nulles (colonnes numériques)
    val imputer = new Imputer()
      .setInputCols(Array("latitude", "longitude", "minimum_nights",
        "number_of_reviews", "reviews_per_month",
        "calculated_host_listings_count", "availability_365",
        "reviews_per_availability", "has_reviews", "log_minimum_nights", "log_availability"))
      .setOutputCols(Array("latitude_imp", "longitude_imp", "minimum_nights_imp",
        "number_of_reviews_imp", "reviews_per_month_imp",
        "calculated_host_listings_count_imp", "availability_365_imp",
        "reviews_per_availability_imp", "has_reviews_imp", "log_minimum_nights_imp", "log_availability_imp"))
      .setStrategy("mean")

    // OneHotEncoder pour colonnes indexées
    val encoder = new OneHotEncoder()
      .setInputCols(Array("neighbourhood_index", "room_type_index"))
      .setOutputCols(Array("neighbourhood_vec", "room_type_vec"))
      .setDropLast(false)

    // VectorAssembler pour assembler toutes les features
    val assembler = new VectorAssembler()
      .setInputCols(Array("neighbourhood_vec", "room_type_vec",
        "latitude_imp", "longitude_imp", "log_minimum_nights_imp",
        "number_of_reviews_imp", "reviews_per_month_imp",
        "calculated_host_listings_count_imp", "log_availability_imp",
        "reviews_per_availability_imp", "has_reviews_imp"))
      .setOutputCol("features")
      .setHandleInvalid("skip")

    // Choix du modèle - RF / on a supp LR (rés catastrophiques)
    val model = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setSeed(42)

    // Pipeline complet
    new Pipeline().setStages(Array(
      neighbourhoodIndexer,
      roomTypeIndexer,
      imputer,
      encoder,
      assembler,
      model
    ))
  }

  // Fonction: Construction grille de paramètres pour Random Forest
  def ParamGrid(pipeline: Pipeline, modelType: String): Array[org.apache.spark.ml.param.ParamMap] = {
    println("\n Construction grille de paramètres")

    val rf = pipeline.getStages.last.asInstanceOf[RandomForestRegressor]
    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.numTrees, Array(100, 150, 200))
      .addGrid(rf.maxDepth, Array(8, 10, 12))
      .addGrid(rf.minInstancesPerNode, Array(5, 10))
      .addGrid(rf.maxBins, Array(32, 64))
      .build()

    println(s"Nbr de combinaisons: ${paramGrid.length}")
    paramGrid
  }

  // Fonction: Entrainement avec Cross Validation
  def model_Training(pipeline: Pipeline,
                      paramGrid: Array[org.apache.spark.ml.param.ParamMap],
                      trainData: org.apache.spark.sql.DataFrame): org.apache.spark.ml.tuning.CrossValidatorModel = {
    println("\n Entrainement du modèle avec CV")

    // Evaluateur RMSE
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    // Cross Validator (validation données créées automatiquement)
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)
      .setParallelism(2)
      .setSeed(42)

    println("Entrainement en cours...")
    val cvModel = cv.fit(trainData)

    println("\n Meilleurs paramètres:")
    val bestModel = cvModel.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
    val rfModel = bestModel.stages.last.asInstanceOf[org.apache.spark.ml.regression.RandomForestRegressionModel]
    
    println(s"Nbr arbres: ${rfModel.getNumTrees}")
    println(s"Max depth: ${rfModel.getMaxDepth}")
    println(s"Min instances per node: ${rfModel.getMinInstancesPerNode}")
    println(s"Max bins: ${rfModel.getMaxBins}")
    
    // Feature importance
    println("\n Feature Importances (Top 10):")
    val featureImportances = rfModel.featureImportances.toArray.zipWithIndex
      .sortBy(-_._1).take(10)
    featureImportances.foreach { case (importance, idx) =>
      println(f"Feature $idx: $importance%.4f")
    }

    cvModel
  }

  // Fonction: Evaluation et affichage prédictions
  def model_Eval(cvModel: org.apache.spark.ml.tuning.CrossValidatorModel,
                    testData: org.apache.spark.sql.DataFrame,
                    modelName: String): Unit = {
    println(s"\n Evaluation du modèle: $modelName")

    // Prédictions sur données de test
    val predictions = cvModel.transform(testData)
      // Transformer back from log scale to original price scale
      .withColumn("label_original", exp(col("label")))
      .withColumn("prediction_original", exp(col("prediction")))

    // Evaluateur RMSE
    val evaluatorRMSE = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    // Evaluateur R2
    val evaluatorR2 = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("r2")

    // Evaluateur MAE
    val evaluatorMAE = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("mae")

    val rmse = evaluatorRMSE.evaluate(predictions)
    val r2 = evaluatorR2.evaluate(predictions)
    val mae = evaluatorMAE.evaluate(predictions)

    println(s"\n Métriques sur données de test (échelle log):")
    println(f"RMSE: $rmse%.2f")
    println(f"R2: $r2%.4f")
    println(f"MAE: $mae%.2f")

    // Evaluations sur l'échelle originale
    val evaluatorRMSE_orig = new RegressionEvaluator()
      .setLabelCol("label_original")
      .setPredictionCol("prediction_original")
      .setMetricName("rmse")

    val evaluatorR2_orig = new RegressionEvaluator()
      .setLabelCol("label_original")
      .setPredictionCol("prediction_original")
      .setMetricName("r2")

    val evaluatorMAE_orig = new RegressionEvaluator()
      .setLabelCol("label_original")
      .setPredictionCol("prediction_original")
      .setMetricName("mae")

    val rmse_orig = evaluatorRMSE_orig.evaluate(predictions)
    val r2_orig = evaluatorR2_orig.evaluate(predictions)
    val mae_orig = evaluatorMAE_orig.evaluate(predictions)

    println(s"\n Métriques sur données de test (échelle originale):")
    println(f"RMSE: $rmse_orig%.2f")
    println(f"R2: $r2_orig%.4f")
    println(f"MAE: $mae_orig%.2f")

    // Affichage des prédictions
    println("\n Prédictions (20 premiers rés):")
    predictions.select("label_original", "prediction_original", "neighbourhood", "room_type",
        "number_of_reviews", "availability_365")
      .show(20, truncate = false)

    // Comparaison label vs prediction
    println("\n Comparaison détaillée:")
    predictions.select(
      col("label_original").alias("Prix_Reel"),
      col("prediction_original").alias("Prix_Predit"),
      (col("prediction_original") - col("label_original")).alias("Erreur"),
      abs((col("prediction_original") - col("label_original")) / col("label_original") * 100).alias("Erreur_%")
    ).show(20, truncate = false)
  }
}