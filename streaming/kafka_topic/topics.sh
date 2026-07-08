#!/bin/bash
# create_topics.sh
# Crée l'ensemble des topics Kafka nécessaires au Level 3, idempotent
# (kafka-topics.sh --create --if-not-exists ne casse rien si déjà présents).
set -e

BROKER="kafka:29092"

topics=(
  # --- Raw : publiés par NiFi ---
  "raw-bank-transactions"
  "raw-insurance-operations"
  "raw-mobile-money-payments"
  "raw-loan-repayments"
  # --- Silver : publiés par le Job 1 Spark Streaming ---
  "silver-bank-transactions"
  "silver-insurance-operations"
  "silver-mobile-money"
  # --- Gold : publiés par le Job 2 Spark Streaming ---
  "gold-fraud-alerts"
  "gold-aml-events"
  "gold-liquidity-alerts"
  # --- Dead Letter Queue : messages malformés rejetés lors de la validation ---
  "dlq-financial-events"
)

echo "⏳ Attente que le port Kafka soit ouvert (check TCP léger, pas de JVM)..."
until bash -c "echo > /dev/tcp/kafka/29092" 2>/dev/null; do
  sleep 2
done
echo "✅ Port Kafka ouvert. Petite marge de sécurité avant de créer les topics..."
sleep 5

for topic in "${topics[@]}"; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000   # 7 jours de rétention, largement suffisant pour la démo
  echo "✅ Topic prêt : $topic"
done

echo "🎉 Tous les topics Kafka sont créés."