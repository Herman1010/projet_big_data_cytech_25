# Taxi Price Prediction – Exercice 5

## Objectif
L’objectif de ce projet est de prédire le **prix total payé pour une course de taxi**
(`total_amount`) à partir de variables explicatives, en mettant en œuvre un
**pipeline de machine learning traditionnel** conforme aux bonnes pratiques MLOps.

---

## Données
Les données utilisées contiennent des informations sur :
- la distance de la course
- le nombre de passagers
- les composantes tarifaires
- la date et l’heure de prise en charge

---

## Feature Engineering
À partir de la date de prise en charge (`tpep_pickup_datetime`), des variables temporelles
ont été créées :
- `hour` : heure de la course
- `weekday` : jour de la semaine
- `month` : mois

Ces variables permettent de capturer les effets temporels sur le prix.

---

## Variable cible
- **Target** : `total_amount`

---

##  Modèle choisi
Après avoir testé plusieurs modèles (régression linéaire, XGBoost, Random Forest),
le modèle retenu pour le rendu final est :

### **Random Forest Regressor**

Ce choix s’explique par :
- sa robustesse face au bruit
- sa capacité à modéliser des relations non linéaires
- son bon compromis entre performance et généralisation

---

## Métriques d’évaluation
Les performances du modèle sont évaluées à l’aide de :
- **RMSE (Root Mean Squared Error)**
- **MAE (Mean Absolute Error)**

###  Résultats obtenus
- **RMSE ≈ 3.68**
- **MAE ≈ 0.16**

Le RMSE est largement inférieur au seuil attendu de 10, indiquant une bonne précision
des prédictions.

---

##  Analyse de corrélation
Une matrice de corrélation a été calculée afin d’analyser la relation entre les variables
explicatives et la variable cible.

Les composantes tarifaires (tarif de base, pourboires, péages, frais aéroport)
présentent une forte corrélation avec le prix total, tandis que les variables temporelles
ont un impact plus faible et non linéaire.

---

##  Visualisations
Deux visualisations ont été réalisées à l’aide de **Matplotlib** :

- **Matrice de corrélation** : analyse des relations entre les variables
- **Graphique valeurs réelles vs valeurs prédites** : évaluation visuelle des performances
  du modèle de régression (équivalent d’une matrice de confusion pour la régression)

---

## Qualité logicielle et MLOps
Le projet respecte les bonnes pratiques MLOps :
- scripts Python (pas de notebook)
- séparation entraînement / inférence
- tests unitaires sur les données d’entrée (entraînement et inférence)
- respect de la norme **PEP8** (`flake8`)
- documentation du code en **NumpyDoc** (`pyment`)
- modèle sauvegardé (`model.pkl`) pour l’inférence

---

## Exécution

```bash
python train.py
pytest
flake8 train.py predict.py
pyment -w train.py
pyment -w predict.py
