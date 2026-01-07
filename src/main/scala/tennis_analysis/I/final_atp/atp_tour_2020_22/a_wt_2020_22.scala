package tennis_analysis.I.final_atp.atp_tour_2020_22

import org.apache.log4j.{Level, Logger}
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, Row}


object a_wt_2020_22 {

  def main(args: Array[String]): Unit = {
    // Supprime les logs Spark pour une sortie plus lisible
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // Initialise une session Spark avec le mode local utilisant tous les CPU disponibles
    val spark = SparkSession.builder()
      .appName("ATP Tennis Analysis 2020-2022")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    val pPath = "Datasets/player_overviews.csv"
    val mPath = "Datasets/match_scores_2020-2022.csv"

    // Créer les Df des Joueurs et des Matches
    val playersDF = createPlayersDF(spark, pPath)
    val matchesDF = createMatchesDF(spark, mPath)

    import org.apache.spark.sql.DataFrame

    //Affichage du schéma + stats Joueurs / Matches
    def printDFInfo(df: DataFrame, name: String, n: Int = 5): Unit = {
      println(s"DataFrame des $name")
      df.printSchema()
      df.show(n, truncate = false)
      println(s"Résumé stats $name :")
      df.describe().show()
    }
    printDFInfo(playersDF, "Joueurs")
    printDFInfo(matchesDF, "Matches")

    // graphe qui représentera tous les matches ATP 2020-2022
    val graph = createMatchesGraph(spark, playersDF, matchesDF)

    // Analyse et statistiques du graphe
    analyzeGraphStatistics(graph)

    // Q3 : Résultats d'un tournoi
    println("\n Résultats du tournoi nitto-atp-finals")
    q3_tournament_results.TournResultst(graph, "nitto-atp-finals", 2020)

    // Q4-5-6 : Analyse ATP Finals 2020
    println("\n Q4-5-6 : Analayse ATP Finals 2020")
    val atpFinalsSubgraph = q4_5_6_atp_finals.createATPFinalSubgraph(graph)
    q4_5_6_atp_finals.visualizeATPFinalSubgraph(atpFinalsSubgraph)
    q4_5_6_atp_finals.analyzeATPFinals(graph)

    // Q7-8 : Analyse des tournois importants
    val tPath = "Datasets/tournaments_2020-2022.csv"
    val tournamentsDF = createTournamentsDF(spark, tPath)

    val importantTournamentsGraph = q7_8_important_tournaments.createImportantTournamentsSubgraph(graph, tournamentsDF)
    println(s"\nSous-graphe créé : ${importantTournamentsGraph.vertices.count()} joueurs, ${importantTournamentsGraph.edges.count()} matches")

    // Affiche les deux alternatives de classement Q8
    q7_8_important_tournaments.displayTop10PlayersAlternative1(importantTournamentsGraph, tournamentsDF)
    q7_8_important_tournaments.displayTop10PlayersAlternative2(importantTournamentsGraph, tournamentsDF)

    spark.stop()
  }

  // Df des joueurs en chargeant et transformant les colonnes importantes
  def createPlayersDF(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .select(
        col("player_id"),
        col("first_name"),
        col("last_name"),
        col("flag_code").alias("nationality"),
        col("residence"),
        col("birthplace"),
        to_date(col("birthdate"), "yyyy.MM.dd").alias("date_of_birth"),
        col("turned_pro"),
        col("weight_kg"),
        col("height_cm"),
        col("handedness"),
        col("backhand")
      )
  }

  // GraphFrames nécessite une colonne 'id' pour les vertices
  private def prepareVerticesDataFrame(playersDF: DataFrame): DataFrame = {
    playersDF
      .select(
        col("player_id").cast("string").alias("id"),
        col("first_name"),
        col("last_name"),
        col("nationality")
      )
      // Ajoute une col pour le fullName (concat first_name et last_name)
      .withColumn(
        "fullName",
        trim(concat_ws(" ", col("first_name"), col("last_name")))
      )
      .distinct()
  }

  // Df des matches en chargeant et transformant les colonnes importantes
  def createMatchesDF(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .select(
        col("match_id"), col("tourn_id"), col("tourn_name"), col("tourn_place"),
        to_date(col("tourn_start_date"), "yyyy.MM.dd").alias("tourn_start_date"),
        to_date(col("tourn_end_date"), "yyyy.MM.dd").alias("tourn_end_date"),
        col("tourn_round"), col("tourn_round_order"), col("winner_id").alias("source"),
        col("loser_id").alias("destination"),
        col("winner_seed").alias("source_seed"),
        col("loser_seed").alias("destination_seed"),
        col("match_score"),
        col("winner_sets").alias("source_sets"),
        col("loser_sets").alias("destination_sets"),
        col("winner_games").alias("source_games"),
        col("loser_games").alias("destination_games"),
        col("winner_tiebreaks_won").alias("source_tiebreaks_won"),
        col("loser_tiebreaks_won").alias("destination_tiebreaks_won"),
        col("tourn_currency"),
        col("tourn_value")
      )
  }

  // Crée le graphe représentant tous les matches ATP de 2020 à 2022 avec GraphFrames
  def createMatchesGraph(spark: SparkSession, playersDF: DataFrame, matchesDF: DataFrame): GraphFrame = {
    // Prépare les vertices avec les colonnes appropriées
    val vertices = prepareVerticesDataFrame(playersDF)
    
    // Filtre les matches pour ne garder que ceux entre 2020-22
    val edges = matchesDF
      .withColumn("tournament_year", year(col("tourn_start_date")))
      .filter(col("tournament_year").between(2020, 2022))
      .select(
        col("source").cast("string").alias("src"),
        col("destination").cast("string").alias("dst"),
        col("tourn_name").alias("tournament"),
        col("tourn_round").alias("round"),
        col("tourn_round_order").alias("roundOrder"),
        col("match_score").alias("matchScore"),
        col("tournament_year").alias("year")
      )

    // Crée et retourne le GraphFrame
    GraphFrame(vertices, edges)
  }

  // Affiche les statistiques et analyse du graphe
  def analyzeGraphStatistics(graph: GraphFrame): Unit = {
    println("\n Stats du graphe ATP 2020-22 ")
    
    // Compte les vertices et edges
    val vertexCount = graph.vertices.count()
    val edgeCount = graph.edges.count()
    
    println(s"\nTaille du graphe:")
    println(s"   Joueurs (vertices) : $vertexCount")
    println(s"   Matches (edges) : $edgeCount")
    
    // Calcule le nbr de matches par tournoi
    val matchesByTournament = graph.edges
      .groupBy(col("tournament"))
      .count()
      .orderBy(col("count").desc)
    
    println(s"\nMatches par tournoi (top 15):")
    matchesByTournament.limit(15).collect().foreach { row =>
      val tournament = row.getAs[String]("tournament")
      val count = row.getAs[Long]("count")
      println(f"   $tournament%-40s: $count%5d matches")
    }
    
    // Calcule le nbr de matches / année
    val matchesByYear = graph.edges
      .groupBy(col("year"))
      .count()
      .orderBy(col("year"))
    
    println(f"\nMatches par année:")
    matchesByYear.collect().foreach { row =>
      val year = row.getAs[Int]("year")
      val count = row.getAs[Long]("count")
      println(f"   $year: $count%6d matches")
    }
    
    // Calcule le nombre de joueurs par pays
    val playersByNationality = graph.vertices
      .groupBy(col("nationality"))
      .count()
      .orderBy(col("count").desc)

  }

  // Crée un DataFrame des tournois à partir du fichier CSV
  def createTournamentsDF(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .select(
        col("tourn_id"),
        col("tourn_name"),
        col("tourn_type"),
        col("tourn_start_year"),
        col("winner_id"),
        col("tourn_winner")
      )
  }

}
