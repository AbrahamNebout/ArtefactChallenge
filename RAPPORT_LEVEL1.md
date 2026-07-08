# Rapport — Level 1 : Foundation (Ingestion & Data Lakehouse Batch)

## 1. Ce qui était demandé

Le challenge Artefact demande de construire, pour un groupe fictif banque/assurance/mobile money (WABA Group, présent dans 8 pays d'Afrique de l'Ouest), une plateforme Data Lakehouse en 4 niveaux progressifs. Le **Level 1** posait le socle :

| Sous-étape | Exigence |
|---|---|
| 1.1 | Une app **Streamlit** générant des données financières fictives (référentiels + 4 types de données transactionnelles), avec sélection multi-pays, volumes paramétrables, et deux modes : *one-time* et *continue* (génération toutes les 10-60s) |
| 1.2 | **MinIO** avec 3 buckets : `raw-landing` (dépôt brut), `lakehouse` (tables Iceberg), `archive` |
| 1.3 | Des jobs **Spark (PySpark)** qui lisent les CSV bruts, valident un schéma explicite, rejettent les lignes malformées, et écrivent dans des tables **Iceberg** partitionnées par `country_code` et date, avec **idempotence garantie** (pas de doublons si un fichier est retraité) |
| 1.4 | Exposition des tables via **Trino**, interrogeables en SQL |

Contraintes transverses à respecter dès ce niveau : cohérence référentielle stricte (`account_id`/`customer_id`/`branch_id` ne doivent jamais être orphelins), pas d'opération destructive incontrôlée, champs `country_code` et `entity_type` partout, code modulaire et lisible.

## 2. Ce qu'on a construit

- **`data-generator/`** : app Streamlit + 4 modules de génération (référentiels, transactions bancaires, opérations d'assurance, mobile money, remboursements de crédit), chacun garantissant la cohérence référentielle par construction (les IDs sont tirés *dans* les référentiels déjà générés, jamais inventés).
- **`spark-jobs/ingest_raw.py`** : un script paramétrable (`--data_type`, `--country`) qui lit un type de donnée, valide son schéma, crée la table Iceberg si besoin, et fait un `MERGE INTO` pour garantir l'idempotence.
- **Infrastructure Docker Compose** : MinIO + Iceberg REST Catalog + Trino + un conteneur Spark "à la demande" (`spark-job-runner`, lancé ponctuellement via `docker exec`).

## 3. Les erreurs rencontrées et comment on les a résolues

Cette partie a été la plus longue — normal, c'est la première fois que toute la chaîne Spark/Iceberg/S3 est assemblée. Voici, dans l'ordre, chaque blocage et sa cause réelle :

### 3.1 — Image `bitnami/spark:3.5.1` introuvable
**Cause** : Bitnami a changé sa politique de distribution en 2025, la plupart des tags versionnés sont passés dans un dépôt payant.
**Fix** : abandon de l'image Bitnami. On repart d'une image `eclipse-temurin` (JRE officiel) + `pip install pyspark`, qui embarque directement le binaire `spark-submit`.

### 3.2 — Chemin `/app/...` corrompu sous Git Bash (Windows)
**Cause** : Git Bash (MINGW64) convertit automatiquement les chemins Unix en chemins Windows, transformant `/app/ingest_raw.py` en `C:/Program Files/Git/app/ingest_raw.py`.
**Fix** : préfixer d'un `/` supplémentaire (`//app/...`) ou `export MSYS_NO_PATHCONV=1`.

### 3.3 — `ClassNotFoundException: IcebergSparkSessionExtensions`
**Cause** : `spark.jars.packages` était déclaré dans le code Python (`.config(...)`), donc *après* le démarrage de la JVM — trop tard, le classpath est déjà figé à ce moment-là.
**Fix** : déplacer cette config dans un fichier `spark-defaults.conf`, lu par `spark-submit` *avant* le démarrage de la JVM (variable d'env `SPARK_CONF_DIR`).

### 3.4 — Conflit de versions Spark 4.1.2 vs jars compilés pour Spark 3.5/Scala 2.12
**Cause** : `pyspark` s'était installé en version 4.1.2 (dernière disponible) au lieu de la version pinnée 3.5.1 demandée — or Spark 4.x tourne sur Scala 2.13, incompatible binairement avec les jars Iceberg/Hadoop-AWS compilés pour Scala 2.12.
**Fix** : `docker compose build --no-cache` pour forcer la réinstallation exacte de `pyspark==3.5.1` depuis `requirements.txt`.

### 3.5 — `ClassNotFoundException: S3AFileSystem`
**Cause** : le filesystem `s3a://` (utilisé pour lire les CSV) nécessite `hadoop-aws` + le SDK AWS **v1**, qu'on n'avait pas encore ajouté à la liste de packages.
**Fix** : ajout de `hadoop-aws` et `aws-java-sdk-bundle` dans `spark-defaults.conf`.

### 3.6 — `NoClassDefFoundError: S3Exception` (SDK v2 manquant)
**Cause** : `S3FileIO`, le composant qu'Iceberg utilise pour lire/écrire les données des tables, est codé pour le SDK AWS **v2** — complètement différent du SDK v1 utilisé par `hadoop-aws`. On avait le v1, pas le v2.
**Fix** : ajout du jar `iceberg-aws-bundle`, qui embarque le SDK v2 nécessaire à `S3FileIO`.
**Point clé à retenir** : dans une stack Iceberg + Hadoop S3A, **deux SDK AWS coexistent** et doivent être fournis séparément.

### 3.7 — Bug `_corrupt_record` : mutation d'un objet Python partagé
**Cause** : `StructType.add()` en PySpark **modifie l'objet en place** plutôt que de retourner une copie. Comme `schema` référençait l'objet global `SCHEMAS["customers"]`, l'appel à `.add("_corrupt_record", ...)` a pollué définitivement ce schéma partagé — la table Iceberg a été créée avec une colonne en trop, provoquant une incohérence au moment du `MERGE`.
**Fix** : construire un nouveau `StructType` via concaténation de listes (`schema.fields + [...]`) plutôt que d'appeler `.add()` sur l'objet partagé.
**Point clé à retenir** : en Python, certaines méthodes "builder" mutent l'objet au lieu de le copier — toujours vérifier ce comportement sur les objets partagés/globaux.

### 3.8 — Catalogue Iceberg REST "in-memory" perdant son état
**Cause** : le service `iceberg-rest` n'a aucun backend de persistance configuré (pas de Postgres/JDBC) — il garde tout en RAM. Après un redémarrage ou un état incohérent, il peut référencer des métadonnées qui n'existent plus réellement dans MinIO.
**Fix** : nettoyage manuel (`mc rm --recursive`) + redémarrage du service pour repartir sur un état propre.
**Point clé à retenir** : c'est une vraie limitation à documenter dans le write-up — en production, on utiliserait un backend persistant.

### 3.9 — `SdkClientException: Unable to load region` puis `Unable to load credentials`
**Cause** : le SDK AWS v2 (utilisé par `S3FileIO`) exige toujours une région et des credentials explicites — même pour un stockage S3-compatible non-AWS comme MinIO. On avait configuré l'endpoint mais pas ces deux éléments, qui sont résolus par une chaîne de providers totalement séparée de celle du SDK v1.
**Fix** : ajout explicite de `spark.sql.catalog.lakehouse.client.region`, `s3.access-key-id`, `s3.secret-access-key` dans la config Spark, plus les variables d'environnement `AWS_REGION`/`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` en filet de sécurité.

### 3.10 — `AnalysisException: Couldn't find column country_code` (table `products`)
**Cause** : le code de création de table supposait que *toutes* les tables avaient une colonne `country_code` à utiliser pour le partitionnement — faux pour `products`, qui est un référentiel partagé entre tous les pays (non partitionné par pays).
**Fix** : détection dynamique (`"country_code" in schema.names`) avant de construire la clause `PARTITIONED BY`.

## 4. Points importants à bien comprendre pour ce niveau

1. **Ordre de génération strict** : référentiels (customers → branches → products → accounts) avant toute donnée transactionnelle. Les générateurs Python appliquent cette contrainte en tirant les IDs *depuis* les référentiels déjà en mémoire, jamais en les inventant — c'est ce qui garantit zéro clé orpheline par construction.

2. **Rôle du catalogue Iceberg REST** : ce n'est pas un stockage de données, seulement de *métadonnées* (schéma, partitions, emplacement des fichiers). Les vraies données (Parquet) vivent dans MinIO. Spark et Trino pointent vers le même catalogue, ce qui leur permet de voir les mêmes tables sans synchronisation manuelle.

3. **Idempotence via `MERGE INTO`** : plutôt qu'un simple `append` (qui dupliquerait les données à chaque re-exécution), on utilise `MERGE INTO ... WHEN NOT MATCHED THEN INSERT` sur la clé métier (`transaction_id`, etc.) — la vraie réponse à l'exigence du cahier des charges.

4. **Schéma explicite plutôt qu'inféré** : `inferSchema=True` est lent (double lecture) et fragile (le type peut dériver selon les données). Un schéma déclaré à la main + mode `PERMISSIVE` + colonne `_corrupt_record` permet de rejeter proprement les lignes malformées, comme demandé.

5. **Partitionnement Iceberg natif** : les fonctions `days(timestamp)` permettent de partitionner par jour directement depuis une colonne timestamp, sans avoir à créer une colonne date dédiée à la main.

6. **Deux SDK AWS dans la même stack** : `hadoop-aws` (lecture des CSV via `s3a://`) utilise le SDK v1 ; `iceberg-aws-bundle` (écriture des tables Iceberg via `S3FileIO`) utilise le SDK v2. Chacun a sa propre chaîne de résolution de credentials/région — à configurer séparément.

7. **`spark.jars.packages` doit être fourni au lancement, pas dans le code** : toute config qui touche au classpath JVM doit être définie *avant* que `spark-submit` démarre la JVM (via `spark-defaults.conf` ou `--packages`), jamais via `.config()` dans le script Python lui-même.

## 5. Bilan pour l'entretien

Ce niveau, bien que "Foundation", a couvert énormément de terrain technique réel : orchestration Docker multi-services, résolution de conflits de dépendances JVM, architecture catalogue/stockage/moteur de requêtage découplée, gestion d'idempotence, et plusieurs pièges Python (mutation d'objets partagés). C'est un bon matériau pour illustrer, à l'oral, une vraie capacité de debug méthodique plutôt qu'un simple "ça a marché du premier coup".
