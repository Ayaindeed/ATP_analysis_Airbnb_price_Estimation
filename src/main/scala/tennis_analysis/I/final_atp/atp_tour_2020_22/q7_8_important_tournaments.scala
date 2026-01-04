package tennis_analysis.I.final_atp.atp_tour_2020_22

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame

// Objet contenant les fonctions pour les Questions 7 et 8
object q7_8_important_tournaments {

  // Q7 : Fonction qui crée un sous-graphe contenant uniquement les matchs des tournois importants
  def createImportantTournamentsSubgraph(graph: GraphFrame, tournamentsDF: DataFrame): GraphFrame = {
    // Définit la liste des catégories de tournois à inclure dans le sous-graphe
    val importantCategories = Seq("Grand Slam", "Masters 1000", "ATP 500")
    
    // Récupère les noms de tournois uniques du graphe
    val graphTournamentNames = graph.edges
      .select(col("tournament"))
      .distinct()
      .collect()
      .map(_.getAs[String](0))
      .toSet
    
    println(s"\nTournois trouvés dans le graphe : ${graphTournamentNames.size}")
    
    // Récupère tous les tournois importants (filtrés par catégorie)
    val importantTournaments = tournamentsDF
      .filter(col("tourn_category").isin(importantCategories: _*))
      .select(col("tourn_name"), col("tourn_category"))
      .distinct()
      .collect()
    
    println(s"Tournois importants trouvés dans le DataFrame : ${importantTournaments.length}")
    
    // Essaye de matcher les tournois du graphe avec les tournois importants
    // en utilisant une correspondance flexible
    val matchedGraphTournaments = graphTournamentNames.filter { graphTourney =>
      importantTournaments.exists { row =>
        val tourneyName = row.getAs[String](0)
        // Match flexible : graphTourney est une version simplifiée/minuscule du tourneyName
        val normalizedName = tourneyName.toLowerCase.replaceAll("\\s+", "-")
          .replaceAll("\\(.*?\\)", "")  // enlever (suspended), (cancelled), etc.
          .replaceAll("-+", "-")         // consolidate tirets
          .replaceAll("^-+|-+$", "")     // trim tirets
        
        graphTourney.contains(normalizedName) || normalizedName.contains(graphTourney)
      }
    }
    
    println(s"Tournois importants trouvés dans le graphe après matching : ${matchedGraphTournaments.size}")
    matchedGraphTournaments.take(20).foreach(t => println(s"  - '$t'"))
    
    // Filtre les arêtes du graphe pour ne garder que les matchs des tournois importants
    val filteredEdges = graph.edges
      .filter(col("tournament").isin(matchedGraphTournaments.toSeq: _*))
    
    // Récupère tous les joueurs impliqués dans les matchs des tournois importants
    val involvedVertices = filteredEdges
      // Sélectionne les IDs des gagnants
      .select(col("src").alias("id"))
      // Ajoute les IDs des perdants
      .union(filteredEdges.select(col("dst").alias("id")))
      // Supprime les doublons pour avoir une liste unique
      .distinct()
    
    // Récupère les informations complètes des joueurs impliqués
    val filteredVertices = graph.vertices
      .join(involvedVertices, "id")
    
    // Crée et retourne le sous-graphe avec les vertices et edges filtrés
    GraphFrame(filteredVertices, filteredEdges)
  }

  // Q8 Alt 1 : Fonction qui affiche le top 10 des joueurs par nombre de victoires
  def displayTop10PlayersAlternative1(importantTournamentsGraph: GraphFrame): Unit = {
    println("\nQ8 - TOP 10 joueurs 2020 - ALt 1 (Par nbr de Ws)")
    
    // nbr de Ws / joueur dans le sous-graphe des tournois imp
    val playerWins = importantTournamentsGraph.edges
      .filter(col("year") === 2020)
      .select(col("src"))
      .groupBy(col("src"))
      .count()
      .withColumnRenamed("src", "id")
      .withColumnRenamed("count", "wins")
      .join(importantTournamentsGraph.vertices.select(col("id"), col("fullName"), col("nationality")), "id")
      .select(col("fullName"), col("nationality"), col("wins"))
      .orderBy(col("wins").desc)
      .limit(10)
    
    var rank = 1
    playerWins.collect().foreach { row =>
      val name = row.getAs[String]("fullName")
      val nationality = row.getAs[String]("nationality")
      val wins = row.getAs[Long]("wins")
      println(f"$rank%2d. $name%-30s ($nationality%-3s) - $wins%3d victoires")
      rank += 1
    }
  }

  // Q8 Alt 2 : F- qui affiche le top 10 des joueurs / pts ATP
  def displayTop10PlayersAlternative2(importantTournamentsGraph: GraphFrame, tournamentsDF: DataFrame): Unit = {
    // Affiche le titre de la section
    println("\nQ8 - TOP 10 joueurs 2020 - Alt 2 (Par pts ATP)")
    
    // Définit les points ATP attribués selon la catégorie du tournoi
    val pointsByCategory = Map(
      "Grand Slam" -> 100,
      "Masters 1000" -> 50,
      "ATP 500" -> 25
    )
    
    // Récupère les noms des tournois du sous-graphe Q7
    val importantTournamentNames = importantTournamentsGraph.edges
      .select(col("tournament"))
      .distinct()
      .collect()
      .map(_.getAs[String](0))
      .toSet
    
    // Filtre le CSV pour ne garder que les gagnants des tournois importants en 2020
    val importantTournamentsWinners = tournamentsDF
      .filter(col("tourn_start_year") === 2020)
      .filter(col("winner_id").isNotNull)
      .filter(col("tourn_category").isin("Grand Slam", "Masters 1000", "ATP 500"))
      .select(col("winner_id"), col("tourn_category"))
    
    val spark = importantTournamentsGraph.vertices.sparkSession
    
    // Calcule les pts pour chaque gagnant
    val playerPointsRDD = importantTournamentsWinners.rdd
      .map { row =>
        val winner_id = row.getAs[String](0)
        val category = row.getAs[String](1)
        val points = pointsByCategory.getOrElse(category, 0)
        (winner_id, points)
      }
      .reduceByKey(_ + _)
    
    // Convertit en DataFrame
    val playerPointsDF = spark.createDataFrame(playerPointsRDD).toDF("id", "points")
    
    // Joint avec les infos des joueurs du sous-graphe Q7
    val playerATPPoints = playerPointsDF
      .join(importantTournamentsGraph.vertices.select(col("id"), col("fullName"), col("nationality")), "id")
      .select(col("id"), col("points"), col("fullName"), col("nationality"))
      .orderBy(col("points").desc)
      .limit(10)
    
    var rank = 1
    playerATPPoints.collect().foreach { row =>
      val name = row.getAs[String]("fullName")
      val nationality = row.getAs[String]("nationality")
      val points = row.getAs[Number]("points").longValue()
      println(f"$rank%2d. $name%-30s ($nationality%-3s) - $points%4d points ATP")
      rank += 1
    }
  }
}
