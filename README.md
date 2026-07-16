# WABA Group — Plateforme Analytique Financière Multi-Pays

## À propos du projet

Ce projet est réalisé dans le cadre d'un challenge de recrutement Data Engineer (Artefact). L'objectif est de construire une plateforme Data Lakehouse complète pour un groupe fictif de banque/assurance/mobile money opérant dans 8 pays d'Afrique de l'Ouest (Côte d'Ivoire, Sénégal, Mali, Burkina Faso, Guinée, Togo, Bénin, Ghana), selon une **architecture médaillon** (Bronze → Silver → Gold) et une **architecture Lambda** (batch + streaming).

Le projet est structuré en 3 niveaux progressifs, **indépendants les uns des autres** (chaque niveau peut être exécuté séparément) :

| Niveau | Objectif | Stack |
|---|---|---|
| **Level 1** | Génération de données + ingestion batch vers un lakehouse Iceberg | Streamlit, MinIO, Spark, Iceberg, Trino |
| **Level 2** | Orchestration du pipeline batch avec architecture médaillon complète | Apache Airflow |
| **Level 3** | Extension temps réel (ingestion streaming, détection de fraude/AML) | Apache NiFi, Apache Kafka, Spark Structured Streaming |
| **Level 4** | Extension temps réel (ingestion streaming, détection de fraude/AML) | Apache NiFi, Apache Kafka, Spark Structured Streaming |

Ce README donne toutes les étapes pour déployer et exécuter le projet de bout en bout, niveau par niveau.

## Pré-requis

- Docker et Docker Compose installés
- Git
- Ports libres sur la machine : `8501`, `9000`, `9001`, `8181`, `8080` (Level 1) ; `8090` (Level 2) ; `8443`, `9092`, `9093`, `8095` (Level 3)
- Sous Windows avec Git Bash : voir la remarque sur les chemins en double-slash (`//app/...`) plus bas — Git Bash convertit automatiquement les chemins Unix, ce qui casse certaines commandes `docker exec` si on ne l'anticipe pas.

## Cloner le projet

```bash
git clone https://github.com/AbrahamNebout/ArtefactChallenge.git
cd ArtefactChallenge/
```

---

## Level 1 — Génération de données & Ingestion Batch

Dossiers concernés : `data-generator/`, `infra/`, `spark-jobs/`.

### 1.1 Lancer l'infrastructure

```bash
docker compose up -d --build
```

Ça démarre l'application de génération de données (Streamlit), MinIO, le catalogue REST Iceberg, Trino, et le conteneur Spark (`waba-spark-runner`).

### 1.2 Générer les données

Accède à l'application : **http://localhost:8501**

**Étape 1 — Référentiels (à générer en premier, ils sont partagés entre tous les pays) :**
1. Ajuste si besoin le nombre de clients/comptes/agences/produits.
2. Clique sur **Générer les référentiels** (un aperçu s'affiche).
3. Clique sur **Envoyer les référentiels vers MinIO**.

**Étape 2 — Données transactionnelles :**
1. Choisis un type de donnée (transactions bancaires, opérations d'assurance, paiements mobile money, ou remboursements de crédit).
2. Ajuste le nombre de lignes et les pays souhaités.
3. Clique sur **Générer et envoyer vers MinIO** (un aperçu s'affiche).
4. Répète pour chacun des 4 types de données.

### 1.3 Accèder à minio
Vérifie ensuite dans MinIO que les fichiers sont bien arrivés : **http://localhost:9001**
Connecter vous avec : 

- user : minioadmin
- pass: minioadmin123


### 1.3 Ingérer les données dans Iceberg via Spark

Dans un terminal :

**Référentiels** (dans cet ordre, `customers` avant `accounts` qui en dépend) :
```bash
docker exec waba-spark-runner spark-submit /app/ingest_raw.py --data_type customers
docker exec waba-spark-runner spark-submit /app/ingest_raw.py --data_type branches
docker exec waba-spark-runner spark-submit /app/ingest_raw.py --data_type products
docker exec waba-spark-runner spark-submit /app/ingest_raw.py --data_type accounts
```

> **⚠️ Sous Git Bash (Windows) :** remplace `/app/ingest_raw.py` par `//app/ingest_raw.py` (double slash) dans **toutes** les commandes `docker exec` de ce README — Git Bash convertit sinon le chemin Unix en chemin Windows et la commande échoue.

Vérification via Trino :
```bash
docker exec -it waba-trino trino --catalog lakehouse --schema bronze --execute "SELECT count(*) FROM lakehouse.bronze.customers"
```

**Données transactionnelles** (`--country` requis) :
```bash
docker exec waba-spark-runner spark-submit /app/ingest_raw.py --data_type bank_transactions --country GH
```

Pour lancer sur tous les pays d'un coup :
```bash
for country in CI SN ML BF GN TG BJ GH; do
  docker exec waba-spark-runner spark-submit /app/ingest_raw.py --data_type bank_transactions --country $country
done
```
Répète (en changeant `--data_type`) pour  `insurance_operations`,  `mobile_money` et `loan_repayments`,.

Vérification finale :
```bash
docker exec -it waba-trino trino --catalog lakehouse --schema bronze --execute "SELECT count(*) FROM lakehouse.bronze.loan_repayments"
```

**✅ Level 1 terminé.**

---

## Level 2 — Orchestration Airflow & Architecture Médaillon

Dossiers concernés : `data-generator/`, `infra/`, `airflow/`.

### 2.1 Régénérer des données fraîches

Les transactions du Level 1 ont été **archivées** lors de leur ingestion (déplacées de `raw-landing` vers `archive`) — il faut donc régénérer de nouveaux données. Les données de référentiels ne sont pas obligatoire, vous pouvez utiliser celle deja generés ou en generé de nouvelles, Mais vous devez obligatoirement generé de nouvelles données transactionnelles depuis l'appli Streamlit (mêmes étapes que 1.2), sans les ré-ingérer manuellement cette fois : c'est Airflow qui va s'en charger.

Conserver la meme date que celle proposer par l'interface, les scripts airflow sont configurés sur cette perriode.

### 2.2 Acceder au dossier et cree le .env

```bash
cd airflow
```
Dans le dossier airflow, cree un ficher .env et inserer ***AIRFLOW_UID=50000***


### 2.3 Démarrer Airflow


```bash
docker compose up -d
```

Patiente environ 5 minutes le temps qu'Airflow initialise sa base et ses services.

Accède à l'interface : **http://localhost:8090** (identifiants : `airflow` / `airflow`)

### 2.4 Charger les Variables Airflow

Dans le menu **Admin → Variables**, importe le fichier `waba_variables.json` (présent dans le dossier `airflow/`). Ces variables contiennent les credentials MinIO et l'URL du catalogue Iceberg, utilisées par toutes les tâches Spark.

### 2.5 Comprendre les DAGs

**Les 4 DAGs d'ingestion** (`dag_ingest_bank_transactions`, `dag_ingest_insurance_operations`, `dag_ingest_mobile_money`, `dag_ingest_loan_repayments`) — un DAG par type de donnée, pour une maintenance et un suivi plus simples qu'un unique DAG monolithique. Chacun est planifié quotidiennement à 01h00 UTC et traite la veille (J-1). Pour chaque pays, 3 tâches en chaîne :
- **`gate`** : filtre optionnel permettant de ne traiter qu'une sélection de pays lors d'un rattrapage manuel (par défaut, tous les pays sont traités).
- **`check_file`** : vérifie qu'au moins un fichier existe pour ce pays et ce jour dans MinIO. Si absent, la tâche est **ignorée** (skip), pas mise en échec — un pays sans données un jour donné est une situation normale, pas une erreur. C'est important : ça permet au DAG entier de rester en succès même si certains pays n'avaient rien à traiter, ce qui conditionne le déclenchement du niveau suivant.
- **`ingest`** : lance le job Spark d'ingestion vers Bronze (ne s'exécute que si `check_file` a trouvé un fichier).

**`dag_bronze_to_silver`** : transforme les 4 tables Bronze en Silver (nettoyage, déduplication, jointures avec les référentiels, conversion des montants en EUR). Planifié à la même cadence que les DAGs d'ingestion, il attend explicitement que les 4 DAGs d'ingestion du jour soient terminés avec succès avant de démarrer ses propres transformations.

**`dag_silver_to_gold`** : calcule les 7 KPIs Gold (financiers et réglementaires) à partir des tables Silver. Même principe : il attend que `dag_bronze_to_silver` du jour soit terminé avec succès avant de se lancer.

**`dag_regulatory_report`** : planifié quotidiennement à J+1 (00h30 UTC), génère le rapport réglementaire consolidé BCEAO/CIMA (taux de créances douteuses + ratio sinistres/primes) à partir des tables Gold déjà calculées.

### 2.6 Conseils d'exécution

- **Au tout premier lancement**, les jars Maven (Iceberg, connecteurs S3) sont téléchargés et mis en cache — pendant cette phase, certaines tâches peuvent échouer une première fois. Patiente quelques secondes : elles redémarrent automatiquement et s'exécutent normalement ensuite.
- Pour éviter les erreurs de démarrage en cascade, lance les DAGs **de façon successive** plutôt que tous en même temps. Si une tâche `gate` passe au rouge, relance-la simplement.
- Une fois les jobs de données transactionnelles exécutés avec succès, les fichiers traités sont **archivés** dans MinIO (déplacés de `raw-landing` vers `archive`) et les tables Iceberg correspondantes sont créées/mises à jour automatiquement dans le lakehouse.

- Vous Pouvez acceder a trino et voir les tables iceberg crées

**✅ Level 2 terminé.**

---

## Level 3 — Pipeline Hybride Batch & Streaming

Tu peux supprimer les conteneurs Airflow (non utilisés dans cette partie), mais **garde l'application de génération de données active** : c'est elle qui alimente ce niveau en temps réel.

Dossier concerné : `streaming/`.

```bash
cd streaming
```

### 3.1 Vue d'ensemble du docker-compose

| Service | Rôle |
|---|---|
| `kafka` | Broker Kafka en mode KRaft (sans Zookeeper), bus de messages central du niveau |
| `kafka-init-topics` | Conteneur one-shot qui crée automatiquement tous les topics Kafka nécessaires au démarrage |
| `kafka-ui` | Interface web pour explorer visuellement les topics et leurs messages |
| `nifi` | Ingestion temps réel : surveille MinIO, publie chaque nouvel événement financier en JSON dans le topic Kafka `raw-*` correspondant |
| `stream-bank-transactions`, `stream-insurance-operations`, `stream-mobile-money` | Job 1 Spark Structured Streaming (un conteneur par type de donnée) : consomment les topics `raw-*`, nettoient/enrichissent, écrivent en double sink (topic `silver-*` + table Iceberg `silver.*`) |
| `stream-fraud-multiple-txn`, `stream-fraud-unusual-country`, `stream-fraud-claim-ratio` | Job 2 Spark Structured Streaming : règles de détection de fraude, publient dans `gold-fraud-alerts` |
| `stream-aml-threshold` | Job 2 : détection des virements dépassant le seuil déclaratif BCEAO/CIMA, publie dans `gold-aml-events` |
| `stream-liquidity-alerts` | Job 2 : surveillance du solde net glissant par pays, publie dans `gold-liquidity-alerts` |

### 3.2 Acceder au dossier er cree le .env

```bash
cd spark_jobs
```
Creer un ficher .env avec le contenu 

```bash
KAFKA_CLUSTER_ID=0yJKRjynSOCyxUyk9sNtZw
NIFI_ADMIN_USERNAME=admin
NIFI_ADMIN_PASSWORD=wabaGroup2026!
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
ICEBERG_CATALOG_URI=http://iceberg-rest:8181
```


### 3.3 Démarrer le stack

```bash
docker compose up -d
```

Ça démarre NiFi, Kafka, et tous les jobs Spark Streaming. Patiente 5-8 minutes environ le temps du téléchargement des images et de l'initialisation des services.

### 3.4 Accéder aux interfaces

**NiFi** : https://localhost:8443/nifi
Le certificat est auto-signé : si le navigateur affiche "Votre connexion n'est pas privée", clique sur **Paramètres avancés** puis **Continuer vers le site**.
Identifiants : `admin` / `wabaGroup2026!`

**Kafka UI** : http://localhost:8095
Les topics doivent déjà apparaître (créés automatiquement au démarrage).

### 3.5 Lancer la génération continue de données

Retourne sur l'application Streamlit (Level 1) :
1. Rafraîchis la page.
2. Régénère de nouveaux référentiels et envoye les vers minio
3. Dans la section données transactionnelles, choisis le mode **Continue**.
4. Ajuste l'intervalle de génération, le nombre de lignes par cycle, et les pays souhaités.
5. Coche **Démarrer la génération continue**.

Les données sont maintenant générées en flux continu dans MinIO.

### 3.6 Charger le flow NiFi

1. Dans la barre d'outils en haut de l'interface NiFi, glisse l'icône **Process Group** (4ᵉ icône) sur l'espace de travail.
2. Clique sur **Browse**, sélectionne le fichier de flow dans `streaming/nifi_flow/`, puis **Add**.
3. Attends qu'il apparaisse sur le canvas, puis double-clique dessus pour l'ouvrir.

### 3.7 Configuration requise après import

**Étape 1 — Credentials MinIO :**
Double-clique sur le processeur **ListS3** → onglet Properties → renseigne :
- Access Key ID : `minioadmin`
- Secret Key : `minioadmin123`
→ **Apply**

Répète exactement la même chose sur le processeur **FetchS3Object** le deuxieme element du job.

**Étape 2 — Activer les Controller Services :**
1. Clic droit sur une zone vide du canvas → **Configure** → onglet **Controller Services**.
2. Pour chacun des 3 services listés : clique sur l'icône ⚡ (éclair) en bout de ligne → **Enable** → **Close**.
3. Une fois les 3 activés, l'icône poubelle (qui indiquait un service non utilisé/inactif) doit disparaître de chaque ligne.
4. Ferme la fenêtre de configuration.

**Étape 3 — Démarrer le flow :**
Sélectionne tous les processeurs (rectangle de sélection sur tout le canvas) → clique sur ▶️ dans le panneau Operate.

### 3.8 Vérifier que tout fonctionne

Retourne sur **Kafka UI** (http://localhost:8095) : les messages doivent commencer à apparaître dans les topics `raw-*`, puis `silver-*`, puis `gold-*` au fur et à mesure que les jobs Spark Streaming les traitent.

**✅ Level 3 terminé.**
