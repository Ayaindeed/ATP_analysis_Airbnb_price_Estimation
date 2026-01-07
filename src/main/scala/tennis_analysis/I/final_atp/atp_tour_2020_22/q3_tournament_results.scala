package tennis_analysis.I.final_atp.atp_tour_2020_22

import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

// Objet contenant les fonctions pour la Question 3
object q3_tournament_results {

  // Fonction qui affiche les résultats d'un tournoi pour une année donnée
  def TournResultst(graph: GraphFrame, tournament: String, year: Int): Unit = {
    // Normalise le nom du tournoi en minuscules et supprime les espaces inutiles
    val tNorm = tournament.trim.toLowerCase
    
    // Filtre les arêtes du graphe pour obtenir uniquement les matchs du tournoi et de l'année spécifiés
    val results = graph.edges
      .filter(lower(col("tournament")) === tNorm && col("year") === year).orderBy(col("roundOrder"))
    
    //  nbr total de matchs trouvés
    val count = results.count()
    // Si aucun match n'est trouvé, affiche un msg
    if (count == 0) {
      println(s"Aucun match trouvé pour '$tournament' en $year")
      return
    }
    
    // Affiche le titre avec le nom du tournoi, l'année et le nombre de matchs
    println(s"\nResultats - $tournament ($year) - $count matches")
    println(f"Year | RoundOrder | Round          | Winner                       | Loser                          | Score")
    println("-" * 130)

    // map qui associe chaque ID de joueur à son nom
    val verticesMap = graph.vertices.select(col("id"), col("fullName")).collect()
      .map(row => (row.getAs[String](0), row.getAs[String](1))).toMap
    
    // Boucle sur chaque match et affiche ses infos
    results.collect().foreach { row =>
      val yr = row.getAs[Int]("year")
      val roundOrder = row.getAs[Int]("roundOrder")
      val round = row.getAs[String]("round")
      val src = row.getAs[String]("src")
      val dst = row.getAs[String]("dst")
      val score = row.getAs[String]("matchScore")
      val winner = verticesMap(src)
      val loser = verticesMap(dst)
      println(f"$yr | $roundOrder%10d | $round%-14s | $winner%-28s | $loser%-28s | $score")
    }
  }
}
