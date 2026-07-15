from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
    IntegerType, BooleanType, DateType,
)


SCHEMAS = {
    "bank_transactions": StructType([
        StructField("transaction_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("account_id", StringType(), False),
        StructField("beneficiary_account", StringType(), False),
        StructField("branch_id", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("transaction_type", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("transaction_status", StringType(), False),
        StructField("fee_amount", DoubleType(), True),
        StructField("entity_type", StringType(), False),
    ]),
    "insurance_operations": StructType([
        StructField("operation_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("customer_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("operation_type", StringType(), False),
        StructField("product_line", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("claim_status", StringType(), True),
        StructField("processing_days", IntegerType(), True),
        StructField("entity_type", StringType(), False),
    ]),
    "mobile_money": StructType([ 
        StructField("payment_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("sender_id", StringType(), False),
        StructField("receiver_id", StringType(), False),
        StructField("sender_country", StringType(), False),
        StructField("receiver_country", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("payment_type", StringType(), False),
        StructField("operator", StringType(), False),
        StructField("status", StringType(), False),
        StructField("fee_amount", DoubleType(), True),
        StructField("entity_type", StringType(), False),
    ]),
    "loan_repayments": StructType([
        StructField("repayment_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("loan_account_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("amount_due", DoubleType(), False),
        StructField("amount_paid", DoubleType(), True),
        StructField("currency", StringType(), False),
        StructField("due_date", DateType(), False),
        StructField("payment_date", DateType(), True),
        StructField("days_overdue", IntegerType(), True),
        StructField("loan_type", StringType(), False),
        StructField("repayment_status", StringType(), False),
        StructField("entity_type", StringType(), False),
    ]),
    "customers": StructType([
        StructField("customer_id", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("entity_type", StringType(), False),
        StructField("segment", StringType(), False),
        StructField("kyc_level", StringType(), False),
        StructField("onboarding_date", DateType(), False),
        StructField("region", StringType(), False),
        StructField("is_active", BooleanType(), False),
    ]),
    "accounts": StructType([
        StructField("account_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("branch_id", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("account_type", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("balance", DoubleType(), False),
        StructField("credit_limit", DoubleType(), True),
        StructField("opened_date", DateType(), False),
        StructField("status", StringType(), False),
    ]),
    "branches": StructType([
        StructField("branch_id", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("entity_type", StringType(), False),
        StructField("city", StringType(), False),
        StructField("region", StringType(), False),
        StructField("branch_type", StringType(), False),
        StructField("is_active", BooleanType(), False),
    ]),
    "products": StructType([
        StructField("product_id", StringType(), False),
        StructField("entity_type", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("is_active", BooleanType(), False),
    ]),
}

# Clé d'idempotence par type de donnée (contrainte du cahier des charges :
# "contrôle sur transaction_id / policy_id / payment_id")
DEDUP_KEYS = {
    "bank_transactions": "transaction_id",
    "insurance_operations": "operation_id",
    "mobile_money": "payment_id",
    "loan_repayments": "repayment_id",
    "customers": "customer_id",
    "accounts": "account_id",
    "branches": "branch_id",
    "products": "product_id",
}


FILE_PREFIXES = {
    "bank_transactions": "bank_txn",
    "insurance_operations": "insurance_ops",
    "mobile_money": "mobile_money",
    "loan_repayments": "loan_repayments",
}