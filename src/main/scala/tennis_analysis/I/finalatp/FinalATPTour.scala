package tennis_analysis.I.finalatp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.{File, PrintWriter}
import scala.sys.process._

case class Player(name: String, country: String)
case class Match(matchType: String, points: Int, head2HeadCount: Int)

object ATPTourAnalysis {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.util.ProcfsMetricsGetter").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ATP Tour Analysis")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val graph = createGraph(sc)
    
    displayGraphInfo(graph)
    generateGraphVisualization(graph)
    
    runAllAnalysis(graph)

    spark.stop()
  }

  // Créer le graphe avec les joueurs et les matchs
  def createGraph(sc: SparkContext): Graph[Player, Match] = {
    val players: RDD[(Long, Player)] = sc.parallelize(Array(
      (1L, Player("Novak Djokovic", "SRB")),
      (3L, Player("Roger Federer", "SUI")),
      (5L, Player("Tomas Berdych", "CZE")),
      (7L, Player("Kei Nishikori", "JPN")),
      (11L, Player("Andy Murray", "GBR")),
      (15L, Player("Stan Wawrinka", "SUI")),
      (17L, Player("Rafael Nadal", "ESP")),
      (19L, Player("David Ferrer", "ESP"))
    ))

    val matches: RDD[Edge[Match]] = sc.parallelize(Array(
      Edge(1L, 5L, Match("G1", 1, 1)),
      Edge(1L, 7L, Match("G1", 1, 1)),
      Edge(3L, 1L, Match("G1", 1, 1)),
      Edge(3L, 5L, Match("G1", 1, 1)),
      Edge(3L, 7L, Match("G1", 1, 1)),
      Edge(7L, 5L, Match("G1", 1, 1)),
      Edge(11L, 19L, Match("G2", 1, 1)),
      Edge(15L, 11L, Match("G2", 1, 1)),
      Edge(15L, 19L, Match("G2", 1, 1)),
      Edge(17L, 11L, Match("G2", 1, 1)),
      Edge(17L, 15L, Match("G2", 1, 1)),
      Edge(17L, 19L, Match("G2", 1, 1)),
      Edge(3L, 15L, Match("S", 5, 1)),
      Edge(1L, 17L, Match("S", 5, 1)),
      Edge(1L, 3L, Match("F", 11, 1))
    ))

    Graph(players, matches)
  }

  // Afficher les informations du graphe au démarrage
  def displayGraphInfo(graph: Graph[Player, Match]): Unit = {
    println("\n ANALYSE DU TOURNOI ATP WORLD TOUR 2015 \n")
    println(s" Nbr de sommets (joueurs): ${graph.vertices.count()}")
    println(s" Nbr d'arêtes (matchs): ${graph.edges.count()}")
    
    println("\nsommets:")
    // sortBy(_._1) trie une collection de tuples par leur 1er élément
    graph.vertices.collect().sortBy(_._1).foreach { case (id, player) =>
      println(s"  $id - ${player.name} (${player.country})")
    }
    
    println("\narretes:")
    graph.edges.collect().foreach { edge =>
      println(s"  ${edge.srcId} -> ${edge.dstId} : type=${edge.attr.matchType}, points=${edge.attr.points}")
    }
  }

  import java.io.{File, PrintWriter}
  import scala.sys.process._

  def generateGraphVisualization(graph: Graph[Player, Match]): Unit = {

    val vertices = graph.vertices.collect().toMap
    val edges    = graph.edges.collect()

    val dotFile = "C:/Users/hp/IdeaProjects/pj_spark/assets/graphe_tournoi.dot"
    val pngFile = "C:/Users/hp/IdeaProjects/pj_spark/assets/graphe_tournoi.png"

    val writer = new PrintWriter(new File(dotFile))

    writer.println(
      """
        |digraph Tournament {
        |  rankdir=TB;
        |  bgcolor="white";
        |  splines=true;
        |  nodesep=0.6;
        |  ranksep=0.8;
        |
        |  node [
        |    shape=box,
        |    style="rounded,filled",
        |    fillcolor="#1E88E5",
        |    fontcolor="white",
        |    fontname="Arial",
        |    fontsize=11
        |  ];
        |
        |  edge [
        |    fontname="Arial",
        |    fontsize=9,
        |    color="#424242",
        |    arrowsize=0.8
        |  ];
        |""".stripMargin
    )

    // Players (vertices)
    vertices.foreach {
      case (id, player) =>
        writer.println(
          s"""  $id [label="${player.name}\\n${player.country}"];"""
        )
    }

    // Matches (edges)
    edges.foreach { edge =>
      val label = s"${edge.attr.matchType} (${edge.attr.points} pts)"

      val (color, penWidth) = edge.attr.matchType match {
        case "F" => ("#D32F2F", 3) // Final
        case "S" => ("#F57C00", 2) // Semi-final
        case _   => ("#388E3C", 1) // Other rounds
      }

      writer.println(
        s"""  ${edge.srcId} -> ${edge.dstId} [
           |    label="$label",
           |    color="$color",
           |    penwidth=$penWidth
           |  ];
           |""".stripMargin
      )
    }

    writer.println("}")
    writer.close()

    try {
      s"dot -Tpng $dotFile -o $pngFile".!!
      println(s"✔ Graph visualization generated: $pngFile")
    } catch {
      case e: Exception =>
        println(s"✖ Error while generating graph: ${e.getMessage}")
    }
  }


  // Exécuter toutes les analyses
  def runAllAnalysis(graph: Graph[Player, Match]): Unit = {
    println("\n RESULTATS DE L'ANALYSE \n")
    
    question1(graph)
    question2(graph)
    question3(graph)
    question4(graph)
    q3(graph)
    q4(graph)
    question5(graph)
    question6(graph)
    question7(graph)
    question8(graph)
    question8_(graph)
    question9(graph)
    question10(graph)
    //question11(graph)
    //question12(graph)
    q11(graph)
    q12(graph)
    question13(graph)
  }

  // Question 1
  // Affiche tous les details des matchs
  def question1(graph: Graph[Player, Match]): Unit = {
    println("1. tous les détails des matchs:")
    graph.edges.collect()
      .sortBy(_.srcId)  // Tri par ID source
      .foreach { edge => // Recupere toutes les aretes
        println(s"   match ${edge.srcId} -> ${edge.dstId}: type=${edge.attr.matchType}, points=${edge.attr.points}, " +
          s"head2head=${edge.attr.head2HeadCount}")
      }
  }

  // Affiche le résume des matchs avec noms des joueurs
  def question2(graph: Graph[Player, Match]): Unit = {
    println("\n2. Liste des matchs:")
    graph.triplets.collect()
      .sortBy(_.srcAttr.name)
      .foreach { triplet => // Recupere tous les triplets (src, edge, dst)
      println(s"   ${triplet.srcAttr.name} a battu ${triplet.dstAttr.name} lors du match ${triplet.attr.matchType}") // Affiche le vainqueur et le perdant
    }
  }

  // Calcule et affiche le classement du groupe 1
  def question3(graph: Graph[Player, Match]): Unit = {
    println("\n3. Vainqueurs du  G1 :")
    val g1_W = graph.triplets // Recupere les triplets
      .filter(t => t.attr.matchType == "G1") // Filtre les matchs du groupe 1
      .map(t => (t.srcId, t.srcAttr.name, t.attr.points)) // Extrait id, nom et pts
      .collect() // Collecte en Array
      .groupBy(_._1) // Groupe par id du joueur
      .map { case (id, matches) => (matches.head._2, matches.map(_._3).sum) } // Calcule la somme des pts
      .toSeq // Convertit en Seq
      .sortBy(-_._2) // Trie par pts decroissants

    g1_W.foreach { case (name, points) => // Parcourt les resultats
      println(s"   $name: $points points")
    }
  }

  // Calcule et affiche le classement du groupe 2
  def question4(graph: Graph[Player, Match]): Unit = {
    println("\n4. Vainqueurs du G2 :")
    val g2_W = graph.triplets // Recupere les triplets
      .filter(t => t.attr.matchType == "G2") // Filtre les matchs du groupe 2
      .map(t => (t.srcId, t.srcAttr.name, t.attr.points)) // Extrait id, nom et pts
      .collect() // Collecte en Array
      .groupBy(_._1) // Groupe par id du joueur
      .map { case (id, matches) => (matches.head._2, matches.map(_._3).sum) } // Calcule la somme des pts
      .toSeq // Convertit en Seq
      .sortBy(-_._2) // Trie par pts decroissants

    g2_W.foreach { case (name, points) => // Parcourt les resultats
      println(s"   $name: $points points")
    }
  }

  def classementParGroupe(
                           graph: Graph[Player, Match],
                           groupType: String,
                           label: String
                         ): Seq[(String, Int)] = {

    println(s"\n$label")

    val winners = graph.triplets
      .filter(t => t.attr.matchType == groupType)
      .map(t => (t.srcId, t.srcAttr.name, t.attr.points))
      .collect()
      .groupBy(_._1)
      .map { case (_, matches) =>
        (matches.head._2, matches.map(_._3).sum)
      }
      .toSeq
      .sortBy(-_._2)

    winners.foreach { case (name, points) =>
      println(s"   $name: $points points")
    }

    winners
  }


  def q3(graph: Graph[Player, Match]): Unit = {
    classementParGroupe(graph, "G1", "3. Vainqueurs du G1 :")
  }

  def q4(graph: Graph[Player, Match]): Unit = {
    classementParGroupe(graph, "G2", "4. Vainqueurs du G2 :")
  }


  // Affiche les resultats des demi-finales
  def question5(graph: Graph[Player, Match]): Unit = {
    println("\n5. Vainqueurs des demi-finales:")
    val semiFinalists = graph.triplets // Recupere les triplets
      .filter(t => t.attr.matchType == "S") // Filtre les matchs de type "S"
      .collect() // Collecte en Array

    semiFinalists.foreach { triplet => // Parcourt les demi-finales
      println(s"   ${triplet.srcAttr.name} , ${triplet.attr.points} points") // Affiche le resultat
      println(s"   ${triplet.srcAttr.name} a battu ${triplet.dstAttr.name} avec ${triplet.attr.points} points") // Affiche le resultat
    }
  }

  // Affiche le vainqueur de la finale
  def question6(graph: Graph[Player, Match]): Unit = {
    println("\n6. Vainqueur de la finale:")
    val finalMatch = graph.triplets // Recupere les triplets
      .filter(t => t.attr.matchType == "F") // Filtre les matchs de type "F" (Finale)
      .collect() // Collecte en Array

    finalMatch.foreach { triplet => // Parcourt la finale
      println(s"  Le légendaire ${triplet.srcAttr.name} avec ${triplet.attr.points} points") // Affiche le vainqueur
      println(s"   ${triplet.srcAttr.name} a remporte la finale contre ${triplet.dstAttr.name} avec ${triplet.attr.points} points") // Affiche le vainqueur
    }
  }

  // Calcule et retourne le total de points par joueur
  def question7(graph: Graph[Player, Match]): Seq[(String, Int)] = {
    println("\n7. Total des points / joueur:")

    val totalPoints = graph.triplets
      .map(t => (t.srcId, t.srcAttr.name, t.attr.points))
      .collect()
      .groupBy(_._1)
      .map { case (_, matches) =>
        (matches.head._2, matches.map(_._3).sum)
      }
      .toSeq
      .sortBy(-_._2)

    totalPoints.foreach { case (name, points) =>
      println(s"   $name: $points points")
    }

    totalPoints // on retourne la valeur
  }

  // Determine le vainqueur du tournoi selon le total de points
  def question8(graph: Graph[Player, Match]): Unit = {
    println("\n8. Vainqueur du tournoi (basée sur le T(points)):")
    val tPoints_ = graph.triplets // Recupere les triplets
      .map(t => (t.srcId, t.srcAttr.name, t.attr.points)) // Extrait id, nom et points
      .collect() // Collecte en Array
      .groupBy(_._1) // Groupe par id du joueur
      .map { case (id, matches) => (matches.head._2, matches.map(_._3).sum) } // Calcule la somme des points
      .toSeq // Convertit en Seq
      .sortBy(-_._2) // Trie par points decroissants

    if (tPoints_.nonEmpty) { // Verifie qu'il y a des resultats
      val (w, pts) = tPoints_.head // Recupere le premier (meilleur score)
      println(s"   $w avec $pts points") // Affiche le vainqueur
    }
  }

  def question8_(graph: Graph[Player, Match]): Unit = {
    val tPoints = question7(graph)
    if (tPoints.nonEmpty) {
      val (w, pts) = tPoints.head
      println("\n8. Vainqueur du tournoi (basée sur le T(points)):")
      println(s"   $w avec $pts points")
    }
  }


  // Identifie les paires de joueurs qui se sont affrontes plusieurs fois
  def question9(graph: Graph[Player, Match]): Unit = {
    println("\n9. Joueurs qui se sont affrontes n fois:")
    val players = graph.vertices.collect().toMap // Recupere tous les joueurs en Map
    val affr = graph.triplets // Recupere les triplets
      .map { t =>
        //val pair = if (t.srcId < t.dstId) (t.srcId, t.dstId) else (t.dstId, t.srcId) // Normalise la paire (ordre croissant)
        val pair = (Math.min(t.srcId, t.dstId), Math.max(t.srcId, t.dstId))
        (pair, 1) // Cree tuple (paire, 1)
      }
      .collect() // Collecte en Array
      .groupBy(_._1) // Groupe par paire
      .map { case (pair, matches) => (pair, matches.length) } // Compte le nbr de matchs
      .filter(_._2 > 1) // Filtre les paires avec plus d'un match

    if (affr.isEmpty) { // Si aucune paire trouvee
      println("   Aucun joueur ne s'est affronte plus d'une fois")
    } else {
      affr.foreach { case ((id1, id2), count) => // Parcourt les paires
        val p1 = players(id1).name // Recupere le nom du p1
        val p2 = players(id2).name // Recupere le nom du p2
        println(s"   $p1 et $p2 ont joue $count fois") // Affiche le resultat
      }
    }
  }

  // Classifie les joueurs selon victoires et defaites
  def question10(graph: Graph[Player, Match]): Unit = {
    println("\n10. Classification des joueurs:")

    val w_ = graph.edges.map(_.srcId).collect().toSet // Set des IDs des vainqueurs
    val l_ = graph.edges.map(_.dstId).collect().toSet // Set des IDs des perdants
    val players = graph.vertices.collect().toMap      // Map des joueurs

    // Fonction locale pour afficher les joueurs filtrés
    def printP(title: String, filterFunc: ((Long, Player)) => Boolean): Unit = {
      println(title)
      players.filter(filterFunc)
        .values                  // Garde juste les p
        .toSeq
        .sortBy(_.name)          // Tri
        .foreach(p => println(s"      ${p.name}"))
    }

    // Appels
      printP("    - Joueurs avec au moins une victoire:",  p => w_.contains(p._1))
    printP("\n  - Joueurs avec au moins une défaite:", p => l_.contains(p._1))
    printP("\n  - Joueurs avec au moins une victoire et une défaite:", p => w_.contains(p._1) && l_.contains(p._1))
  }


//  // Identifie les joueurs sans aucune victoire
//  def question11(graph: Graph[Player, Match]): Unit = {
//    println("\n11. Joueurs sans victoire:")
//    val w = graph.edges.map(_.srcId).collect().toSet // Set des ids de vainqueurs
//    val pl = graph.vertices.collect().toMap // Map des joueurs
//    val noW = pl.filter(p => !w.contains(p._1)) // Filtre ceux qui ne sont pas dans winners
//
//    if (noW.isEmpty) { // Si liste vide
//      println("   Tous les joueurs ont au moins une victoire") // Affiche message
//    } else {
//      noW.foreach { case (id, player) => // Parcourt les joueurs sans victoire
//        println(s"   ${player.name}")
//      }
//    }
//  }
//
//  // Identifie les joueurs sans aucune defaite
//  def question12(graph: Graph[Player, Match]): Unit = {
//    println("\n12. Joueurs sans défaite:")
//    val losers = graph.edges.map(_.dstId).collect().toSet // Set des ids de perdants
//    val players = graph.vertices.collect().toMap // Map des joueurs
//    val noLosses = players.filter(p => !losers.contains(p._1)) // Filtre ceux qui ne sont pas dans losers
//
//    if (noLosses.isEmpty) { // Si liste vide
//      println("   Tous les joueurs ont au moins une defaite") // Affiche message
//    } else {
//      noLosses.foreach { case (id, player) => // Parcourt les joueurs sans defaite
//        println(s"   ${player.name}") // Affiche le nom
//      }
//    }
//  }

  // opt: Identifie les joueurs sans aucune victoire - utilise RDD operations
  def q11(graph: Graph[Player, Match]): Unit = {
    println("\n11. Joueurs sans victoire :")
    val noW = graph.vertices // Recupere tous les sommets (joueurs)
      .leftOuterJoin(graph.edges.map(_.srcId).distinct().map((_, true))) // Join avec ids de vainqueurs
      .filter(_._2._2.isEmpty) // Garde ceux qui n'ont pas de victoire
      .map(_._2._1) // Extrait le joueur
      .collect() // Collecte en Array
      .sortBy(_.name) // Tri par nom
    
    if (noW.isEmpty) println("   Tous les joueurs ont au moins une victoire") // Affiche message
    else noW.foreach(p => println(s"   ${p.name}")) // Affiche les joueurs sans victoire
  }

  // opt: Identifie les joueurs sans aucune defaite - utilise RDD operations
  def q12(graph: Graph[Player, Match]): Unit = {
    println("\n12. Joueurs sans défaite :")
    val noL = graph.vertices // Recupere tous les sommets (joueurs)
      .leftOuterJoin(graph.edges.map(_.dstId).distinct().map((_, true))) // Join avec ids de perdants
      .filter(_._2._2.isEmpty) // Garde ceux qui n'ont pas de defaite
      .map(_._2._1) // Extrait le joueur
      .collect() // Collecte en Array
      .sortBy(_.name) // Tri par nom
    
    if (noL.isEmpty) println("   Tous les joueurs ont au moins une défaite") // Affiche message
    else noL.foreach(p => println(s"   ${p.name}")) // Affiche les joueurs sans defaite
  }


  def question13(graph: Graph[Player, Match]): Unit = {
    println("\n13. Les 3 joueurs les + importants (PageRank):")

    // Récupère tous les sommets (joueurs) et les convertit en Map(id -> Player)
    val players = graph.vertices.collectAsMap()

    // Calcule les 3 joueurs avec le meilleur PageRank
    val topPageRank = graph.reverse // Inverse les arêtes (v  <-> d)
      .pageRank(0.001) // critère de convergence, + petit + précis, mais + calcul
      .vertices // Récupère les sommets avec leurs scores PR
      .takeOrdered(3)(Ordering[Double].reverse.on(_._2)) // Prend les 3 meilleurs scores (tri décroissant sur le rank)
      .map { case (id, rank) => (players(id).name, rank) } // Convertit (id, rank) en (nom, rank)

    // Affiche les 3 joueurs avec leur score PR
    topPageRank.foreach { case (name, rank) =>
      println(f"   $name: pagerank = $rank%.3f")
    }

    // Compare avec le nbr  de victoires
    println("\n   Comparaison avec victoires:")
    graph.edges.map(_.srcId) // Extrait l'ID du vainqueur
      .countByValue() // Compte le nbr  d'occ de chaque ID (= nbr de W)
      .toSeq // Convertit la Map en Seq pour pouvoir trier
      .sortBy(-_._2) // Trie par nbr de W décroissant
      .take(3) // Prend les 3
      .foreach { case (id, count) => // Pour chaque joueur du top 3
        println(f"   ${players(id).name}: $count victoires") // Affiche nom et nbr de victoires
      }
  }
}
