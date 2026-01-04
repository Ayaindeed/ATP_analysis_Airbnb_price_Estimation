package tennis_analysis.I.final_atp.atp_tour_2020_22

import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

object q4_5_6_atp_finals {

  // Q4 : Fonction qui crée un sous-graphe contenant uniquement les matchs d'un tournoi pour une année donnée
  def createATPFinalSubgraph(graph: GraphFrame): GraphFrame = {
    // Filtre les arêtes du graphe pour les matchs de nitto-atp-finals en 2020
    val filteredEdges = graph.edges
      .filter(lower(col("tournament")) === "nitto-atp-finals" && col("year") === 2020)
    
    // Récupère tous les joueurs impliqués dans les matchs filtrés
    val involvedVertices = filteredEdges
      // Sélectionne les IDs des gagnants
      .select(col("src").alias("id"))
      // Ajoute les IDs des perdants à la liste précédente
      .union(filteredEdges.select(col("dst").alias("id")))
      // Supprime les doublons
      .distinct()
    
    // Récupère les informations complètes des joueurs
    val filteredVertices = graph.vertices
      .join(involvedVertices, "id")
    
    // Crée et retourne le sous-graphe avec les vertices et edges filtrés
    GraphFrame(filteredVertices, filteredEdges)
  }

  // Visualise le sous-graphe ATP Finals dans le terminal
  def visualizeATPFinalSubgraph(atpFinalsGraph: GraphFrame): Unit = {
    println("\nVisualisation du sous-graphe ATP Finals")
    
    // Affiche les joueurs (vertices)
    println("\n Joueurs (vertices)")
    val vertices = atpFinalsGraph.vertices
      .select(col("id"), col("fullName"), col("nationality"))
      .collect()
    
    vertices.foreach { row =>
      val id = row.getAs[String]("id")
      val name = row.getAs[String]("fullName")
      val nationality = row.getAs[String]("nationality")
      println(s"  $id: $name ($nationality)")
    }
    
    // Affiche les arêtes (matchs)
    println(s"\n Matchs (edges)- Total: ${atpFinalsGraph.edges.count()} ---")
    val edges = atpFinalsGraph.edges
      .select(col("src"), col("dst"), col("round"))
      .collect()
    
    edges.foreach { row =>
      val winner = row.getAs[String]("src")
      val loser = row.getAs[String]("dst")
      val round = row.getAs[String]("round")
      println(s"  $winner -> $loser (Round: $round)")
    }
    
    // Statistiques du graphe
    println("\nStats")
    println(s"  Nombre de joueurs: ${vertices.length}")
    println(s"  Nombre de matchs: ${edges.length}")
  }

  // Q5-Q6 : Fonction qui analyse les ATP Finals (groupes et qualifications)
  def analyzeATPFinals(graph: GraphFrame): Unit = {
    // Crée le sous-graphe des ATP Finals pour nitto-atp-finals 2020
    val atpFinalsGraph = createATPFinalSubgraph(graph)
    
    // Compte le nombre de joueurs et de matchs dans le sous-graphe
//    val playerCount = atpFinalsGraph.vertices.count()
//    val matchCount = atpFinalsGraph.edges.count()
//    // Affiche les statistiques du sous-graphe
//    println(s"\nSous-graphe nitto-atp-finals 2020 : $playerCount joueurs, $matchCount matches")
//
    // Q5 : Détecte les joueurs de phase de groupe
    // 4j / grp
    val groupPlayers = atpFinalsGraph.vertices
      .join(atpFinalsGraph.edges.select(col("src").alias("id")).union(atpFinalsGraph.edges.select(col("dst").alias("id"))).distinct(), "id")
      .select(col("id"), col("fullName"), col("nationality"))
    
    // Trie et divise en deux groupes de 4 joueurs chacun
    val allGroupPlayers = groupPlayers.orderBy(col("id")).collect()
    val groupA = allGroupPlayers.slice(0, 4)
    val groupB = allGroupPlayers.slice(4, 8)
    
    println(s"\nQ5 - Détection des 2 groupes (nitto-atp-finals 2020)")
    println("\nGroupe A :")
    groupA.foreach { row =>
      val name = row.getAs[String]("fullName")
      val nationality = row.getAs[String]("nationality")
      println(s"  - $name ($nationality)")
    }
    
    println("\nGroupe B :")
    groupB.foreach { row =>
      val name = row.getAs[String]("fullName")
      val nationality = row.getAs[String]("nationality")
      println(s"  - $name ($nationality)")
    }
    
    // Q6 : Calcule le nombre de victoires pour chaque joueur
    val playerWins = atpFinalsGraph.edges
      .select(col("src").alias("id"))
      .groupBy("id")
      .count()
      .withColumnRenamed("count", "wins")
    
    // Fonction helper pour afficher les 2 joueurs qualifiés d'un groupe
    def showQualifiedPlayers(group: Array[org.apache.spark.sql.Row], groupName: String): Unit = {
      val groupIds = group.map(_.getAs[String]("id")).toList
      val groupIdsDF = groupPlayers.filter(col("id").isin(groupIds: _*))
      
      val qualified = groupIdsDF
        .join(playerWins, "id")
        .orderBy(col("wins").desc)
        .limit(2)
      
      println(s"\nGroupe $groupName - 2 joueurs qualifiés (par victoires) :")
      qualified.collect().foreach { row =>
        val name = row.getAs[String]("fullName")
        val wins = row.getAs[Long]("wins")
        println(s"  $name : $wins victoire(s)")
      }
    }
    
    println(s"\nQ6 - Joueurs qualifiés pour les demi-finales (nitto-atp-finals 2020)")
    showQualifiedPlayers(groupA, "A")
    showQualifiedPlayers(groupB, "B")
  }
}
