package tennis_analysis.I.final_atp.atp_tour_2020_22

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame

object q7_8_important_tournaments {

  // Q7 : Crée un sous-graphe filtré par les tournois importants (toutes années 2020-2022)
  def createImportantTournamentsSubgraph(graph: GraphFrame, tournamentsDF: DataFrame): GraphFrame = {
    val importantCategories = Seq("Grand Slam", "Masters 1000", "ATP 500")
    
    // Récupère les noms de tournois importants  (toutes années)
    val importantTournamentNames = tournamentsDF
      .filter(col("tourn_type").isin(importantCategories: _*))
      .select(col("tourn_name"))
      .distinct()
      .collect()
      .map(_.getAs[String](0).toLowerCase)
      .toSet
    
    println(s"Tournois importants trouvés dans CSV: ${importantTournamentNames.size}")
    
    // Filtre les arêtes du graphe pour les tournois importants (toutes années)
    val filteredEdges = graph.edges
      .filter(lower(col("tournament")).isin(importantTournamentNames.toSeq: _*))
    
    println(s"Matches dans tournois importants: ${filteredEdges.count()}")
    
    // Affiche les matches par tournoi
    println("\nMatches par tournoi important:")
    val matchesByTournament = filteredEdges
      .groupBy(col("tournament"))
      .count()
      .orderBy(col("count").desc)
    
    matchesByTournament.collect().foreach { row =>
      val tournament = row.getAs[String]("tournament")
      val count = row.getAs[Long]("count")
      println(f"  - $tournament%-45s: $count%4d matches")
    }
    
    // Récupère les joueurs impliqués dans ces matchs
    val involvedVertices = filteredEdges
      .select(col("src").alias("id"))
      .union(filteredEdges.select(col("dst").alias("id")))
      .distinct()
    
    val filteredVertices = graph.vertices
      .join(involvedVertices, "id")
    
    GraphFrame(filteredVertices, filteredEdges)
  }

  // Q8 Alt 1 : Top 10 joueurs par nombre de titres de tournois gagnés en 2020 dans les tournois importants
  def displayTop10PlayersAlternative1(importantGraph: GraphFrame, tournamentsDF: DataFrame): Unit = {
    println("\nQ8 - TOP 10 joueurs 2020 - Alt1 (Par nbr de titres de tournois)")
    
    val importantCategories = Seq("Grand Slam", "Masters 1000", "ATP 500")
    val winners2020 = tournamentsDF
      .filter(col("tourn_start_year") === 2020)
      .filter(col("tourn_type").isin(importantCategories: _*))
      .filter(col("winner_id").isNotNull && col("tourn_winner").isNotNull)
    
    val top10 = winners2020
      .groupBy(col("winner_id"), col("tourn_winner"))
      .count()
      .withColumnRenamed("count", "titles")
      .orderBy(col("titles").desc)
      .limit(10)
    
    var rank = 1
    top10.collect().foreach { row =>
      val name = row.getAs[String]("tourn_winner")
      val titles = row.getAs[Long]("titles")
      println(f"$rank%2d. $name%-30s - $titles%2d titres")
      rank += 1
    }
  }

  // Q8 Alt 2 : Top 10 joueurs par points ATP (titres pondérés) en 2020
  def displayTop10PlayersAlternative2(importantGraph: GraphFrame, tournamentsDF: DataFrame): Unit = {
    println("\nQ8 - TOP 10 joueurs 2020 - Alt2 (Par points ATP)")
    
    // Définit les points par type de tournoi
    val pointsCol = when(col("tourn_type") === "Grand Slam", 100)
      .when(col("tourn_type") === "Masters 1000", 50)
      .when(col("tourn_type") === "ATP 500", 25)
      .otherwise(0)
    
    val importantCategories = Seq("Grand Slam", "Masters 1000", "ATP 500")
    val winners2020 = tournamentsDF
      .filter(col("tourn_start_year") === 2020)
      .filter(col("tourn_type").isin(importantCategories: _*))
      .filter(col("winner_id").isNotNull && col("tourn_winner").isNotNull)
      .withColumn("points", pointsCol)
    
    val top10 = winners2020
      .groupBy(col("winner_id"), col("tourn_winner"))
      .agg(sum(col("points")).alias("total_points"))
      .orderBy(col("total_points").desc)
      .limit(10)
    
    var rank = 1
    top10.collect().foreach { row =>
      val name = row.getAs[String]("tourn_winner")
      val points = row.getAs[Long]("total_points")
      println(f"$rank%2d. $name%-30s - $points%4d points ATP")
      rank += 1
    }
    
    // Affiche le détail des tournois gagnés
    println("\nDétail des tournois gagnés (2020)")
    top10.collect().foreach { row =>
      val playerId = row.getAs[String]("winner_id")
      val name = row.getAs[String]("tourn_winner")
      val points = row.getAs[Long]("total_points")
      
      println(f"\n$name ($points points):")
      
      val playerTournaments = winners2020
        .filter(col("winner_id") === playerId)
        .select(col("tourn_name"), col("tourn_type"), col("points"))
        .collect()
      
      playerTournaments.foreach { tourn =>
        val tournName = tourn.getAs[String]("tourn_name")
        val tournType = tourn.getAs[String]("tourn_type")
        val tournPoints = tourn.getAs[Int]("points")
        println(f"  - $tournName%-40s ($tournType%-15s) = $tournPoints%3d points")
      }
    }
  }
}
