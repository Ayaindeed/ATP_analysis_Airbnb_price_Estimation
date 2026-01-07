# Projet Final - Apache Spark.

Deux projets analytiques utilisant  : analyse de graphes pour les tournois ATP (GraphX/GraphFrames) et prédiction de prix avec machine learning pour Airbnb (MLlib).

---

## Structure du Projet

```
pj_spark/
├── src/
│   └── main/
│       └── scala/
│           ├── AirbnbPriceEstimation/
│           │   └── AirbnbPriceEst.scala
│           └── tennis_analysis/
│               └── I/
│                   ├── finalatp(GraphX)
│                   │   ├── FinalATPTour.scala
│                   └── atp_tour_2020_22(GraphFrames)
│                       └── /a_wt_2020_22.scala
├── Datasets/
│   ├── airbnb-data.csv
│   ├── match_scores_2020-2022.csv
│   ├── player_overviews.csv
│   └── tournaments_2020-2022.csv
├── assets/
│   └── graphe_tournoi_atp.dot
├── build.sbt
└── README.md
```

---

## 1. Analyse des Tournois de Tennis ATP 🎾

- Modélisation des joueurs et de leurs matchs sous forme de graphes pour extraire des insights sur les performances, les classements et les relations entre joueurs.

### Objectif
- Analyser les données des tournois ATP (2020-2022) en représentant les joueurs comme des nœuds et les matchs comme des arêtes.

### Technologies utilisées
- **GraphX** : Modélisation et analyse des graphes
- **GraphFrames** : Requêtes avancées et patterns de graphes

### Fichiers
- Tournois: [Datasets/tournaments_2020-2022.csv](Datasets/tournaments_2020-2022.csv)
- Scores: [Datasets/match_scores_2020-2022.csv](Datasets/match_scores_2020-2022.csv)
- Joueurs: [Datasets/player_overviews.csv](Datasets/player_overviews.csv)
- Graphique: [assets/graphe_tournoi.dot](assets/graphe_tournoi.dot)

---

## 2. Estimation des Prix Airbnb 合 $ˎˊ˗

- Prédiction des prix des annonces Airbnb avec deux modèles pour comparaison.

### Objectif
- Créer des modèles de régression pour estimer le prix d'une annonce Airbnb basé sur ses caractéristiques (localisation, type de chambre, nombre d'avis, disponibilité, etc.).

### Pipeline ML

1. **Chargement & EDA** : Analyse exploratoire des données
2. **Préparation des données** :
   - Concaténation des colonnes `host_id` et `id`
   - Conversion `number_of_reviews` → integer
   - Conversion `reviews_per_month` et `price` → double
   - Suppression de `host_id` et `neighbourhood_group`
   - Filtrage des valeurs nulles (`neighbourhood`, `room_type`)

3. **Transformateurs & Estimateurs** :
   - `StringIndexer` : Conversion des colonnes catégorielles en indices
   - `Imputer` : Remplissage des valeurs nulles par la moyenne
   - `OneHotEncoder` : Encodage des colonnes indexées
   - `VectorAssembler` : Assemblage des features

4. **Modèles pour comparaison** :
   - **Random Forest Regressor** : Capte bien les interactions, robuste aux outliers
   - **Linear Regression** : Résultats catastrophiques / non utilisé

### Fichiers
- Source: [src/main/scala/AirbnbPriceEstimation/AirbnbPriceEst.scala](src/main/scala/AirbnbPriceEstimation/AirbnbPriceEst.scala)
- Dataset: [Datasets/airbnb-data.csv](Datasets/airbnb-data.csv)

---

## Installation & Utilisation

### Prérequis
- Scala 2.12+
- Apache Spark 3.x+
- sbt (Scala Build Tool)

### Compiler & Exécuter
```bash
sbt compile
sbt "run"
```

### Configuration
- [build.sbt](build.sbt)
