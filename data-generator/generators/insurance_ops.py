"""
generators/insurance_ops.py

Génère les opérations d'assurance (insurance_ops_[CC]_YYYYMMDD_NN.csv).
Particularité : claim_status n'a de sens QUE pour les opérations liées
à un sinistre (CLAIM_SUBMISSION, CLAIM_PAYMENT) -> NULL sinon.
"""
import numpy as np
import pandas as pd
import uuid
from datetime import datetime

from config import OPERATION_TYPES, PRODUCT_LINES, CLAIM_STATUSES, CURRENCY_MAP

RNG = np.random.default_rng()

CLAIM_OPERATION_TYPES = {"CLAIM_SUBMISSION", "CLAIM_PAYMENT"}


def _random_timestamps(start, end, n):
    delta_seconds = int((end - start).total_seconds())
    offsets = RNG.integers(0, delta_seconds, size=n)
    return np.array([start + pd.Timedelta(seconds=int(o)) for o in offsets])


def generate_insurance_operations(
    customers: pd.DataFrame,
    accounts: pd.DataFrame,
    country: str,
    n_rows: int,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    # On restreint aux clients/comptes du pays ET de l'entité INSURANCE
    country_customers = customers.loc[
        (customers.country_code == country) & (customers.entity_type == "INSURANCE"),
        "customer_id"
    ].values
    country_accounts = accounts.loc[
        accounts.country_code == country, "account_id"
    ].values

    if len(country_customers) == 0:
        raise ValueError(
            f"Aucun client INSURANCE pour {country} — vérifie la répartition "
            f"des entity_type dans generate_customers()."
        )

    customer_id = RNG.choice(country_customers, size=n_rows)
    account_id = RNG.choice(country_accounts, size=n_rows)
    operation_type = RNG.choice(
        OPERATION_TYPES, size=n_rows,
        p=[0.45, 0.20, 0.15, 0.15, 0.05]  # paiements de prime = flux le + fréquent
    )
    product_line = RNG.choice(
        PRODUCT_LINES, size=n_rows,
        p=[0.30, 0.25, 0.15, 0.20, 0.10]
    )
    amount = np.maximum(1000, RNG.lognormal(mean=11, sigma=1.3, size=n_rows)).round(0)

    # --- Logique métier clé : claim_status et processing_days ---
    # Seules les opérations "sinistre" ont un statut de traitement.
    is_claim = np.isin(operation_type, list(CLAIM_OPERATION_TYPES))

    claim_status = np.where(
        is_claim,
        RNG.choice(CLAIM_STATUSES, size=n_rows, p=[0.20, 0.35, 0.10, 0.35]),
        None,
    )
    # processing_days : uniquement rempli si c'est un sinistre traité (PAID/REJECTED)
    is_processed_claim = is_claim & np.isin(claim_status, ["PAID", "REJECTED"])
    processing_days = np.where(
        is_processed_claim,
        RNG.integers(1, 45, size=n_rows),  # délai de traitement en jours ouvrés
        None,
    )

    df = pd.DataFrame({
        "operation_id": [str(uuid.uuid4()) for _ in range(n_rows)],
        "timestamp": _random_timestamps(start, end, n_rows),
        "customer_id": customer_id,
        "account_id": account_id,
        "country_code": country,
        "operation_type": operation_type,
        "product_line": product_line,
        "amount": amount,
        "currency": CURRENCY_MAP[country],
        "claim_status": claim_status,
        "processing_days": processing_days,
        "entity_type": "INSURANCE",
    })

    return df