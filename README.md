# Ingestion de données depuis datos.gob.es

Ce projet implémente un pipeline d’**ingestion de données** depuis le portail [datos.gob.es](https://datos.gob.es), qui expose les jeux de données publics espagnols via une API.  
Les données récupérées sont stockées dans une base **SQLite** et exportées au format **CSV**.

---

## 📌 Qu’est-ce que la Data Ingestion ?

La **data ingestion** est le processus qui consiste à collecter des données depuis une ou plusieurs sources (APIs, bases, fichiers, flux temps réel, etc.) et à les intégrer dans un système cible (base de données, data lake, entrepôt de données).  

Il existe deux modes principaux :

- **Batch ingestion** : les données sont collectées périodiquement (par lots). Exemple : lancer ce script une fois par jour pour mettre à jour les jeux de données.
- **Real-time ingestion** : les données sont collectées en continu, à mesure qu’elles sont produites (flux Kafka, WebSockets, etc.). Exemple : surveiller un flux d’événements en direct et l’intégrer immédiatement.

Dans ce projet, l’ingestion est réalisée en **batch** (pages successives d’API).

---

## ⚙️ Fonctionnalités principales

- Récupération des jeux de données et de leurs distributions depuis l’API de datos.gob.es.
- Normalisation des champs texte.
- Stockage en base **SQLite** :
  - Table `datasets`  
  - Table `distributions`  
  - Table `geo_taxonomy` (taxonomie NTI des couvertures géographiques)
- Export des données au format **CSV** dans le dossier `./data`.

---

## 📚 Description des fonctions

### Connexion et schéma de base
- **`mk_conn(db_path="datosgob.db")`**  
  Crée une base SQLite, supprime les anciennes tables si elles existent, et définit la structure des tables :
  - `datasets`
  - `distributions`
  - `geo_taxonomy`

---

### Récupération et normalisation
- **`fetch_page(page=0)`**  
  Interroge l’API datos.gob.es pour une page donnée, avec paramètres `_pageSize` et `_page`.

- **`normalize_text(value)`**  
  Transforme un champ (string, dict, list) en une chaîne lisible.  
  Permet d’unifier les formats hétérogènes de l’API.

---

### Extraction des données
- **`extract_dataset(item)`**  
  Construit un dictionnaire Python normalisé pour un dataset (id, titre, description, dates, éditeur, JSON brut).

- **`extract_distributions(item, dataset_id)`**  
  Extrait les distributions associées à un dataset (id, format, URLs, JSON brut).

---

### Sauvegarde
- **`save_to_db(conn, dataset, distributions)`**  
  Insère ou met à jour un dataset et ses distributions dans SQLite.

- **`insert_geo_entities(conn)`**  
  Insère dans `geo_taxonomy` un ensemble d’entités NTI (pays, régions, provinces).  
  Peut être adapté pour récupérer directement depuis l’API NTI.

---

### Export
- **`export_csv(conn, out_dir="data")`**  
  Exporte les tables `datasets` et `distributions` en CSV.

- **`export_geo_csv(conn, out_dir="data")`**  
  Exporte la table `geo_taxonomy` en CSV.

---

### Orchestration
- **`main(max_pages=10)`**  
  Pipeline principal :
  1. Initialise la base (`mk_conn`)  
  2. Charge la taxonomie NTI (`insert_geo_entities`)  
  3. Parcourt les pages de l’API (`fetch_page`)  
  4. Extrait et sauvegarde les datasets/distributions  
  5. Exporte en CSV (`export_csv`, `export_geo_csv`)

---

## 🚀 Exécution

```bash
python ingest_datosgob.py
