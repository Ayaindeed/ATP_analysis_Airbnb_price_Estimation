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
   - **Linear Regression** : Baseline rapide et efficace pour petit dataset, avec régularisation (Ridge/Lasso)

5. **Validation** : Cross-Validation 5-fold
6. **Hyperparamètres optimisés** :
   
   **Random Forest** :
   - `numTrees`: [50, 100, 150]
   - `maxDepth`: [5, 10, 15]
   - `minInstancesPerNode`: [1, 5]
   - **Total**: 18 combinaisons
   

7. **Métriques d'évaluation** : RMSE, R², MAE

### Résultats du Modèle Random Forest

**Meilleurs paramètres trouvés:**
- Nombre d'arbres: **200**
- Profondeur maximale: **12**
- Min instances par nœud: **5**
- Max bins: **32**

**Feature Importances (Top 10):**
| Feature | Importance |
|---------|-----------|
| Feature 39 | 0.2945 |
| Feature 40 | 0.1338 |
| Feature 45 | 0.0962 |
| Feature 49 | 0.0854 |
| Feature 50 | 0.0639 |
| Feature 47 | 0.0631 |
| Feature 48 | 0.0497 |
| Feature 46 | 0.0471 |
| Feature 51 | 0.0446 |
| Feature 52 | 0.0344 |

**Métriques (Échelle Log):**
- RMSE: **0.45**
- R²: **0.5528**
- MAE: **0.32**

**Métriques (Échelle Originale - $):**
- RMSE: **187.42**
- R²: **0.2367**
- MAE: **73.92**

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
