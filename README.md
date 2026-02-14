# Projet Big Data - Ingénierie 3

## Auteur
- Nom : Herman SESSOU, Edner GOMA, Nour BEN MILED, Thibault GARCIA-MEGEVAND 
- Filière : Ingénierie Informatique  
- Année : 3ème année  

## Description du projet
L'objectif de ce projet est de déployer une architecture Big Data complète pour se familiariser avec les environnements industriels. Le flux de données simule une chaîne de production réelle :

* **Data Lake (Minio) :** Stockage des données brutes (fichiers Parquet).
* **Data Warehouse (PostgreSQL) :** Ingestion et structuration des données préparées.
* **Intégration :** Traitement via Spark pour valider et transformer la donnée.
* **Machine Learning :** Modèle de prédiction du prix des courses (Target: `total_amount`).

## Structure du projet
```text
├── docker-compost.yml      # Infrastructure (Minio, Postgres) 
├── data/                   # Cycle de vie de la donnée 
│   ├── raw/                # Données immuables (NYC Taxi Parquet)
│   ├── interim/            # Données transformées intermédiaires 
│   └── processed/          # Données finales pour le modeling 
├── spark_data_integration/ # [Exercice 1] Collecte et upload vers Minio
├── spark_data_integration_exercice2/ # [Exercice 2] Ingestion multi-branche 
├── spark_data_integration_exercice3/ # [Exercice 3] Modèle SQL 
├── dataviz_exercice4/      # [Exercice 4] Dashboard de visualisation 
└── machine-learning_exercice5/ # [Exercice 5] Script ML (RMSE < 10) 
Les exercices 1 et 2 sont faits ensemble dans la classe main.scala de l'exercice2.
```


## Fichier Power BI

Le fichier `Exo_4_dashboard.zip` contenant le tableau de bord Power BI **fait plus de 100 Mo et ne peut pas être inclus dans le dépôt GitHub**.  
Vous pouvez le télécharger via ce lien externe :  
## [Télécharger le dashboard Power BI] (https://drive.google.com/file/d/18l2ZdyYkN37SxHW5uYQg7Ng9sHwj69-7/view?usp=sharing)  

---
## Prérequis

- Python
- Sbt  
- Power BI Desktop pour ouvrir le tableau de bord
- Docker : Lancer l'infra avec docker compose up -d

---

