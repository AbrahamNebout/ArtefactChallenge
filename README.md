# WABA Group — Plateforme Analytique Financière Multi-Pays

## À propos du projet

Ce projet est réalisé dans le cadre d'un challenge de recrutement Data Engineer (Artefact). L'objectif est de construire une plateforme Data Lakehouse complète pour un groupe fictif de banque/assurance/mobile money opérant dans 8 pays d'Afrique de l'Ouest (Côte d'Ivoire, Sénégal, Mali, Burkina Faso, Guinée, Togo, Bénin, Ghana), selon une **architecture médaillon** (Bronze → Silver → Gold) et une **architecture Lambda** (batch + streaming).

Le projet est structuré en 4 niveaux progressifs, **indépendants les uns des autres** (chaque niveau peut être exécuté séparément) :

| Niveau | Objectif | Stack |
|---|---|---|
| **Level 1** | Génération de données + ingestion batch vers un lakehouse Iceberg | Streamlit, MinIO, Spark, Iceberg, Trino |
| **Level 2** | Orchestration du pipeline batch avec architecture médaillon complète | Apache Airflow |
| **Level 3** | Extension temps réel (ingestion streaming, détection de fraude/AML) | Apache NiFi, Apache Kafka, Spark Structured Streaming |
| **Level 4** | Déploiement et monitoring sur Kubernetes | K8s, Streamlit, MinIO, Spark, Iceberg, Trino, Apache NiFi, Apache Kafka, Spark Structured Streaming |

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

Cette commande démarre l'application de génération de données (Streamlit), MinIO, le catalogue REST Iceberg, Trino, et le conteneur Spark (`waba-spark-runner`).

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

### 1.3 Accéder à MinIO

Vérifie ensuite dans MinIO que les fichiers sont bien arrivés : **http://localhost:9001**

Connecte-toi avec :
- user : `minioadmin`
- pass : `minioadmin123`

### 1.4 Ingérer les données dans Iceberg via Spark

Dans un terminal :

**Référentiels** (dans cet ordre — `customers` avant `accounts`, qui en dépend) :
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

Répète (en changeant `--data_type`) pour `insurance_operations`, `mobile_money` et `loan_repayments`.

Vérification finale :
```bash
docker exec -it waba-trino trino --catalog lakehouse --schema bronze --execute "SELECT count(*) FROM lakehouse.bronze.loan_repayments"
```

**✅ Level 1 terminé.**

---

## Level 2 — Orchestration Airflow & Architecture Médaillon

Dossiers concernés : `data-generator/`, `infra/`, `airflow/`.

### 2.1 Régénérer des données fraîches

Les transactions du Level 1 ont été **archivées** lors de leur ingestion (déplacées de `raw-landing` vers `archive`) — il faut donc régénérer de nouvelles données. Les référentiels ne sont pas obligatoires (tu peux réutiliser ceux déjà générés ou en créer de nouveaux), mais tu dois obligatoirement régénérer de nouvelles données transactionnelles depuis l'application Streamlit (mêmes étapes qu'en 1.2), sans les réingérer manuellement cette fois : c'est Airflow qui va s'en charger.

Conserve la même date que celle proposée par l'interface — les DAGs Airflow sont configurés sur cette période.

### 2.2 Créer le fichier .env

```bash
cd airflow
```

Dans le dossier `airflow`, crée un fichier `.env` et insère :
```
AIRFLOW_UID=50000
```

### 2.3 Démarrer Airflow

**Important** — trouve le GID du groupe `docker` sur ta machine hôte :

```bash
stat -c '%g' /var/run/docker.sock
```

Puis, dans le fichier `docker-compose.override.yaml`, ajoute le numéro trouvé :

```yaml
airflow-worker:
  volumes:
    - /var/run/docker.sock:/var/run/docker.sock
  group_add:
    - "999"   # remplace 999 par le GID trouvé ci-dessus
```

Puis :

```bash
docker compose up -d
```

Patiente environ 5 minutes le temps qu'Airflow initialise sa base et ses services.

Accède à l'interface : **http://localhost:8090** (identifiants : `airflow` / `airflow`)

### 2.4 Charger les variables Airflow

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
- Tu peux accéder à Trino pour voir les tables Iceberg créées.


Vérification des tables exposées via trino :
```bash
docker exec -it waba-trino trino --catalog lakehouse --schema bronze --execute "SHOW TABLES FROM lakehouse.bronze"

docker exec -it waba-trino trino --catalog lakehouse --schema silver --execute "SHOW TABLES FROM lakehouse.silver"

docker exec -it waba-trino trino --catalog lakehouse --schema gold --execute "SHOW TABLES FROM lakehouse.gold"
```

**✅ Level 2 terminé.**

---

## Level 3 — Pipeline Hybride Batch & Streaming

Tu peux supprimer les conteneurs Airflow (non utilisés dans cette partie), mais **garde l'application de génération de données active** : c'est elle qui alimente ce niveau en temps réel.
Dans le dossier airflow, tapé 

```bash
docker compose down -v
```


Dossier concerné : `streaming/`.

```bash
cd ..
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

### 3.2 Créer le fichier .env à la racine du dossier streaming

```
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
Le certificat est auto-signé : si le navigateur affiche « Votre connexion n'est pas privée », clique sur **Paramètres avancés** puis **Continuer vers le site**.
Identifiants : `admin` / `wabaGroup2026!`

**Kafka UI** : http://localhost:8095
Les topics doivent déjà apparaître (créés automatiquement au démarrage).

### 3.5 Lancer la génération continue de données

Retourne sur l'application Streamlit (Level 1) :
1. Rafraîchis la page.
2. Régénère de nouveaux référentiels et envoie-les vers MinIO.
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
Double-clique sur le processeur **ListS3** (le premier processor) → onglet Properties → renseigne :
- Access Key ID : `minioadmin`
- Secret Key : `minioadmin123`
→ **Apply**

Répète exactement la même chose sur le processeur **FetchS3Object** (le deuxième processor).

**Étape 2 — Activer les Controller Services :**
1. Clic droit sur une zone vide du canvas → **Configure** → onglet **Controller Services**.
2. Pour chacun des 3 services listés : clique sur l'icône ⚡ (éclair) en bout de ligne → **Enable** → **Close**.
3. Une fois les 3 activés, l'icône poubelle (qui indiquait un service non utilisé/inactif) doit disparaître de chaque ligne.
4. Ferme la fenêtre de configuration.

**Étape 3 — Démarrer le flow :**
Sélectionne tous les processeurs (rectangle de sélection sur tout le canvas) → clique sur ▶️ dans le panneau Operate.

### 3.8 Vérifier que tout fonctionne

Retourne sur **Kafka UI** (http://localhost:8095) : les messages doivent commencer à apparaître dans les topics `raw-*`, puis `silver-*`, puis `gold-*` au fur et à mesure que les jobs Spark Streaming les traitent.

Verifier les tables crées dans trino 


NB: les tables crées via les jobs streaming se terminent par _stream
```bash
docker exec -it waba-trino trino --catalog lakehouse --schema bronze --execute "SHOW TABLES FROM lakehouse.bronze"

docker exec -it waba-trino trino --catalog lakehouse --schema silver --execute "SHOW TABLES FROM lakehouse.silver"

docker exec -it waba-trino trino --catalog lakehouse --schema gold --execute "SHOW TABLES FROM lakehouse.gold"
```


**✅ Level 3 terminé.**

---

## Level 4 — Déploiement sur Kubernetes

En raison de ressources insuffisantes, le déploiement se fait selon trois scénarios possibles, à choisir en fonction des ressources disponibles :

- **Option A** : partie batch uniquement (MinIO, Airflow, Spark Operator, Trino, Iceberg, application de génération de données).
- **Option B** : partie streaming uniquement (MinIO, NiFi, Kafka, Trino, Iceberg, jobs de traitement streaming, application de génération de données).
- **Option C** : infrastructure complète.

### 4.1 Prérequis — création des secrets

Se placer dans le dossier `K8s` :

```bash
cd K8s
```

Créer les deux secrets nécessaires :

```bash
kubectl create secret generic minio-credentials \
  --from-literal=rootUser=minioadmin \
  --from-literal=rootPassword='UnVraiMotDePasseSolide!'

kubectl create secret generic grafana-admin-credentials \
  --from-literal=admin-user=admin \
  --from-literal=admin-password='UnVraiMotDePasseSolide!'
```

### 4.2 Configuration de l'accès Git (git-sync)

Ouvrir le fichier `git-sync-secret.yaml` et remplacer :
```
GIT_SYNC_USERNAME
GIT_SYNC_PASSWORD
```
par les valeurs envoyées par email.

Puis appliquer ce secret :

```bash
kubectl apply -f git-sync-secret.yaml
```

---

### 4.3 Option A — Déploiement de la partie batch

#### 4.3.1 Déploiement du chart Helm

```bash
cd data-platform-batch
kubectl apply -f spark-serviceaccount-and-pvc.yaml
helm dependency list
helm lint .
helm install data-platform-batch .
```

Patiente jusqu'au déploiement complet des composants. Un déploiement correct crée automatiquement les buckets suivants dans MinIO :
`raw-landing`, `archive`, `lakehouse`, `airflow-logs`.

⚠️ En cas de manque de ressources, le pod de provisioning MinIO peut échouer à créer ces buckets — vérifie et crée-les manuellement si nécessaire.

#### 4.3.2 Génération des données et accès à MinIO

Accède à l'application de génération de données via :
`http://<ip-du-cluster>:30911`

Le processus de génération des données est le même que celui du Level 1.

Puis accède à MinIO via `http://<ip-du-cluster>:32001`, et connecte-toi avec :
Identifiants : `minioadmin` / `UnVraiMotDePasseSolide!`

Accède à Trino via `http://<ip-du-cluster>:30906`, et connecte-toi avec :
Identifiants : `admin`


#### 4.3.3 Configuration initiale d'Airflow

Accède à l'interface Airflow via `http://<ip-du-cluster>:31151`, et connecte-toi avec :
Identifiants : `admin` / `admin`

Rends-toi dans **Admin → Variables** puis :
- importe le fichier `waba-variables-batch.json` présent dans le dossier `K8s/data-platform-batch`.

#### 4.3.4 Créer les connexions

Rends-toi dans **Admin → Connections** et crée deux connexions :

**Connexion Kubernetes**
- **Connection Id** : `kubernetes_default`
- **Connection Type** : `Kubernetes Cluster Connection`
- Cocher **In cluster configuration**
- **Namespace** : `default`

**Connexion MinIO (pour les logs distants)**
- **Connection Id** : `minio_s3_conn`
- **Connection Type** : `Amazon Web Services`
- **aws_access_key_id** : `minioadmin`
- **aws_secret_access_key** : `UnVraiMotDePasseSolide!`
- **Champs supplémentaires JSON** :
```json
{
  "endpoint_url": "http://data-platform-batch-minio:9000"
}
```

#### 4.3.5 Architecture des DAGs

**DAGs d'ingestion Bronze** : `dag_ingest_bank_transactions`, `dag_ingest_insurance_operations`, `dag_ingest_loan_repayments`, `dag_ingest_mobile_money`

Ingestion quotidienne à 01h00 UTC, traitant les données de J-1 (`logical_date`). Pour chaque pays, pipeline en 3 étapes :
- **`gate_{country}`** : `ShortCircuitOperator` qui filtre selon le paramètre `country_codes` (permet un rattrapage sélectif via déclenchement manuel).
- **`check_file_{country}`** : vérifie la présence du fichier CSV du jour avant de lancer le traitement.
- **`ingest_{DATA_TYPE}_{country}`** : soumet un `SparkApplication` (CRD Spark Operator) qui exécute réellement l'ingestion vers Bronze.

**`dag_bronze_to_silver`** : attend, via `ExternalTaskSensor`, que les 4 DAGs d'ingestion aient terminé avec succès leur run du jour, puis lance en parallèle les transformations Silver (nettoyage, déduplication, jointures référentielles, conversion en EUR) pour chaque type de donnée.

**`dag_silver_to_gold`** : attend la fin de `dag_bronze_to_silver`, puis calcule 7 KPIs métier/réglementaires (volume de transactions, ratio NPL, ARPU, loss ratio, etc.) en parallèle.

**`dag_regulatory_report`** : DAG indépendant (planifié 30 minutes avant les autres), génère le rapport réglementaire quotidien BCEAO/CIMA.

**Pourquoi CeleryExecutor plutôt que KubernetesExecutor**

J'ai choisi le **CeleryExecutor** pour éviter de créer un pod Kubernetes pour chaque tâche Airflow — ce que ferait le KubernetesExecutor, y compris pour des tâches très légères comme `gate` ou `check_file`.

Avec CeleryExecutor :
- Les tâches `gate_{country}` et `check_file_{country}` s'exécutent **directement sur le worker Celery** (un simple processus Python), sans création de pod dédié.
- La tâche `ingest_{DATA_TYPE}_{country}` s'exécute aussi *depuis* le worker Celery, mais son rôle est différent : elle **soumet un objet `SparkApplication`** au cluster Kubernetes (via l'API du Spark Operator). C'est le **Spark Operator**, et non Airflow, qui se charge alors de créer le **pod driver Spark** (et les pods executors si besoin) pour exécuter réellement le job.
- Une fois le job Spark terminé, le pod driver est **automatiquement supprimé** par le Spark Operator après le délai `TTL_SECONDS_AFTER_FINISHED`.

**Résultat** : seuls les jobs Spark, qui ont réellement besoin de ressources de calcul isolées, génèrent des pods K8s. Les tâches d'orchestration légères (gate, check_file, sensors) restent sur les workers Celery, ce qui réduit fortement le nombre de pods créés/détruits et la charge sur le cluster.

#### 4.3.6 Déclenchement des DAGs

Retourne dans Airflow et déclenche les DAGs.

> ⚠️ Pour des raisons de ressources limitées, déclenche les DAGs **un par un, au fur et à mesure**, plutôt que tous en même temps — commence par les jobs d'ingestion de données.

---

### 4.4 Option B — Déploiement de la partie streaming

Une fois tes tests terminés, tu peux supprimer le déploiement précédent et déployer la partie streaming.

Les jobs de traitement streaming nécessitent le PVC `spark-ivy-cache-pvc`, créé normalement par Spark au démarrage. Si tu as démarré la partie streaming directement (sans passer par l'option batch), vérifie et crée-le si nécessaire :

```bash
kubectl get pvc

kubectl apply -f - <<'EOF'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: spark-ivy-cache-pvc
  namespace: default
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: standard
  resources:
    requests:
      storage: 2Gi
EOF
```

Vérifie également le secret MinIO, et recrée-le si nécessaire :
```bash
kubectl create secret generic minio-credentials \
  --from-literal=rootUser=minioadmin \
  --from-literal=rootPassword='UnVraiMotDePasseSolide!'
```

#### 4.4.1 Déploiement du chart Helm

```bash
cd data-platform-stream
helm dependency list
helm lint .
helm install data-platform-stream .
```

Tu auras ainsi déployé :

| Service | URL |
|---|---|
| Data Generator (Streamlit) | `http://<ip-du-cluster>:30911` |
| Kafka | `<ip-du-cluster>:30902` |
| Kafka UI | `http://<ip-du-cluster>:30903` |
| MinIO | `http://<ip-du-cluster>:32001` |
| NiFi | `https://<ip-du-cluster>:30905` |
| Trino | `http://<ip-du-cluster>:30906` |
| Iceberg | — |

⚠️ Un déploiement correct crée automatiquement les topics suivants :
`raw-bank-transactions`, `raw-insurance-operations`, `raw-mobile-money-payments`, `raw-loan-repayments`, `silver-bank-transactions`, `silver-insurance-operations`, `silver-mobile-money`, `dlq-financial-events`, `gold-liquidity-alerts`, `gold-aml-events`, `gold-fraud-alerts`.

⚠️ Et les buckets suivants dans MinIO :
`raw-landing`, `archive`, `lakehouse`, `airflow-logs`.

⚠️ En cas de ressources insuffisantes, le pod de provisioning peut échouer à créer ces ressources — vérifie et crée-les manuellement si nécessaire.

#### 4.4.2 Configuration de NiFi

Accède à l'interface NiFi via `https://<ip-du-cluster>:30905`, et connecte-toi avec :
Identifiants : `username` / `changemechangeme`

1. Dans la barre d'outils en haut de l'interface NiFi, glisse l'icône **Process Group** (4ᵉ icône) sur l'espace de travail.
2. Clique sur **Browse**, sélectionne le fichier `K8s/data-platform-stream/NiFi_Flow_k8s.json`, puis **Add**.
3. Attends qu'il apparaisse sur le canvas, puis double-clique dessus pour l'ouvrir.

**Configuration requise après import :**

**Étape 1 — Credentials MinIO :**
Double-clique sur le processeur **ListS3** → onglet Properties → renseigne :
- Access Key ID : `minioadmin`
- Secret Key : `UnVraiMotDePasseSolide!`
→ **Apply**

Répète exactement la même chose sur le processeur **FetchS3Object** (le deuxième élément du job).

**Étape 2 — Activer les Controller Services :**
- Clic droit sur une zone vide du canvas → **Configure** → onglet **Controller Services**.
- Pour chacun des 3 services listés : clique sur l'icône ⚡ (éclair) en bout de ligne → **Enable** → **Close**.
- Une fois les 3 activés, l'icône poubelle (qui indiquait un service non utilisé/inactif) doit disparaître de chaque ligne.
- Ferme la fenêtre de configuration.

**Étape 3 — Démarrer le flow :**
Sélectionne tous les processeurs (rectangle de sélection sur tout le canvas) → clique sur ▶️ dans le panneau Operate.

#### 4.4.3 Générer les données en continu

Retourne sur l'application Streamlit (Level 1) :
- Rafraîchis la page.
- Régénère de nouveaux référentiels et envoie-les vers MinIO.
- Dans la section données transactionnelles, choisis le mode **Continue**.
- Ajuste l'intervalle de génération, le nombre de lignes par cycle, et les pays souhaités.
- Coche **Démarrer la génération continue**.

Les données sont maintenant générées en flux continu dans MinIO.

#### 4.4.4 Vérifier que tout fonctionne

Retourne sur **Kafka UI** (`http://<ip-du-cluster>:30903`) : les messages doivent commencer à apparaître dans les topics `raw-*`, puis `silver-*`, puis `gold-*` au fur et à mesure que les jobs Spark Streaming les traitent.

---

### 4.5 Option C — Déploiement de l'infrastructure complète

Si tu disposes de ressources suffisantes pour déployer l'ensemble de l'infrastructure, utilise les commandes suivantes :

```bash
cd data-platform
kubectl apply -f spark-serviceaccount-and-pvc.yaml
helm dependency list
helm lint .
helm install data-platform .
```

**Points d'attention en cas de ressources insuffisantes lors du déploiement complet :**

1. **Le déploiement peut être long** selon les ressources disponibles sur le cluster.
2. **Migration Airflow** : si les migrations de base de données ne se terminent pas correctement, les pods des composants Airflow ne démarreront pas. Vérifie avec `kubectl get pods` et relance si besoin.
3. **Buckets MinIO** : un déploiement correct crée automatiquement les buckets `raw-landing`, `lakehouse` et `archive`. En cas de manque de ressources, le pod de provisioning peut ne pas démarrer — dans ce cas, crée les buckets manuellement une fois le déploiement terminé.
4. **Topics Kafka** : de même, un déploiement correct crée automatiquement les topics suivants : `raw-bank-transactions`, `raw-insurance-operations`, `raw-mobile-money-payments`, `raw-loan-repayments`, `silver-bank-transactions`, `silver-insurance-operations`, `silver-mobile-money`, `dlq-financial-events`, `gold-liquidity-alerts`, `gold-aml-events`, `gold-fraud-alerts`.
5. **Buckets MinIO** : de même, un déploiement correct crée automatiquement les buckets suivants dans MinIO : `raw-landing`, `archive`, `lakehouse`, `airflow-logs`.

⚠️ Vérifie que les topics et les buckets sont bien créés avant de continuer ; si ce n'est pas le cas, crée-les toi-même.

#### 4.5.1 Génération des données et accès à MinIO

Accède à l'application de génération de données via :
`http://<ip-du-cluster>:30911`

Le processus de génération des données est le même que celui du Level 1.

Puis accède à MinIO via `http://<ip-du-cluster>:32001`, et connecte-toi avec :
Identifiants : `minioadmin` / `UnVraiMotDePasseSolide!`

#### 4.5.2 Configuration initiale d'Airflow

Accède à l'interface Airflow via `http://<ip-du-cluster>:31151`, et connecte-toi avec :
Identifiants : `admin` / `admin`

Rends-toi dans **Admin → Variables** puis :
- importe le fichier `waba-variables.json` présent dans le dossier `K8s/data-platform`.

Rends-toi dans **Admin → Connections** et crée deux connexions :

**Connexion Kubernetes**
- **Connection Id** : `kubernetes_default`
- **Connection Type** : `Kubernetes Cluster Connection`
- Cocher **In cluster configuration**
- **Namespace** : `default`

**Connexion MinIO (pour les logs distants)**
- **Connection Id** : `minio_s3_conn`
- **Connection Type** : `Amazon Web Services`
- **aws_access_key_id** : `minioadmin`
- **aws_secret_access_key** : `UnVraiMotDePasseSolide!`
- **Champs supplémentaires JSON** :
```json
{
  "endpoint_url": "http://data-platform-minio:9000"
}
```

#### 4.5.3 Configuration de NiFi

Accède à l'interface NiFi via `https://<ip-du-cluster>:30905`, et connecte-toi avec :
Identifiants : `username` / `changemechangeme`

1. Dans la barre d'outils en haut de l'interface NiFi, glisse l'icône **Process Group** (4ᵉ icône) sur l'espace de travail.
2. Clique sur **Browse**, sélectionne le fichier `K8s/data-platform-stream/NiFi_Flow_k8s.json`, puis **Add**.
3. Attends qu'il apparaisse sur le canvas, puis double-clique dessus pour l'ouvrir.

**Configuration requise après import :**

**Étape 1 — Credentials MinIO :**
Double-clique sur le processeur **ListS3** (premier processeur) → onglet Properties → renseigne :
- Access Key ID : `minioadmin`
- Secret Key : `UnVraiMotDePasseSolide!`
- Endpoint Override URL : `http://data-platform-minio:9000`
→ **Apply**

Répète exactement la même chose sur le processeur **FetchS3Object** (deuxième processeur).

**Étape 2 — Credentials Kafka :**
Double-clique sur le processeur **PublishKafkaRecord_2_6** (dernier processeur) → onglet Properties → renseigne :
- Kafka Brokers : `data-platform-kafka:9092`
→ **Apply**

**Étape 3 — Activer les Controller Services :**
- Clic droit sur une zone vide du canvas → **Configure** → onglet **Controller Services**.
- Pour chacun des 3 services listés : clique sur l'icône ⚡ (éclair) en bout de ligne → **Enable** → **Close**.
- Une fois les 3 activés, l'icône poubelle (qui indiquait un service non utilisé/inactif) doit disparaître de chaque ligne.
- Ferme la fenêtre de configuration.

**Étape 4 — Démarrer le flow :**
Sélectionne tous les processeurs (rectangle de sélection sur tout le canvas) → clique sur ▶️ dans le panneau Operate.

#### 4.5.4 Générer les données en continu

Retourne sur l'application Streamlit (Level 1) :
- Rafraîchis la page.
- Régénère de nouveaux référentiels et envoie-les vers MinIO.
- Dans la section données transactionnelles, choisis le mode **Continue**.
- Ajuste l'intervalle de génération, le nombre de lignes par cycle, et les pays souhaités.
- Coche **Démarrer la génération continue**.

Les données sont maintenant générées en flux continu dans MinIO.

#### 4.5.5 Vérifier que tout fonctionne

Retourne sur **Kafka UI** (`http://<ip-du-cluster>:30903`) : les messages doivent commencer à apparaître dans les topics `raw-*`, puis `silver-*`, puis `gold-*` au fur et à mesure que les jobs Spark Streaming les traitent.

#### 4.5.6 Déclenchement des DAGs

Retourne dans Airflow et déclenche les DAGs.

> ⚠️ Pour des raisons de ressources limitées, déclenche les DAGs **un par un, au fur et à mesure**, plutôt que tous en même temps — commence par les jobs d'ingestion de données.
