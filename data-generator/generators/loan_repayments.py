"""
generators/loan_repayments.py

Génère les remboursements de prêts (loan_repayments_[CC]_YYYYMMDD_NN.csv).
Contrainte du schéma A.7 : loan_account_id doit exister dans accounts.csv
AVEC account_type = 'LOAN' (pas n'importe quel compte).
"""
import numpy as np
import pandas as pd
import uuid
from datetime import datetime, timedelta

from config import LOAN_TYPES, REPAYMENT_STATUSES, REPAYMENT_STATUS_PROBS, CURRENCY_MAP

RNG = np.random.default_rng()


def generate_loan_repayments(
    customers: pd.DataFrame,
    accounts: pd.DataFrame,
    country: str,
    n_rows: int,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    # Seuls les comptes de type LOAN peuvent avoir des remboursements
    loan_accounts = accounts.loc[
        (accounts.country_code == country) & (accounts.account_type == "LOAN"),
        ["account_id", "customer_id"]
    ]

    if len(loan_accounts) == 0:
        raise ValueError(
            f"Aucun compte LOAN pour {country} — vérifie account_type "
            f"dans generate_accounts()."
        )

    # On tire n_rows lignes DANS ce sous-ensemble (compte + client restent liés)
    sampled = loan_accounts.sample(n=n_rows, replace=True, random_state=None)
    loan_account_id = sampled["account_id"].values
    customer_id = sampled["customer_id"].values

    loan_type = RNG.choice(LOAN_TYPES, size=n_rows, p=[0.40, 0.15, 0.20, 0.10, 0.15])
    amount_due = np.maximum(1000, RNG.lognormal(mean=11.5, sigma=1.2, size=n_rows)).round(0)

    repayment_status = RNG.choice(
        REPAYMENT_STATUSES, size=n_rows, p=REPAYMENT_STATUS_PROBS
    )


    delta_seconds = max(1, int((end - start).total_seconds()))
    due_offsets = RNG.integers(0, delta_seconds, size=n_rows)
    due_date = np.array([start + timedelta(seconds=int(o)) for o in due_offsets])

    # --- Logique métier : montant payé, jours de retard, date de paiement ---
    # dépendent tous les trois du repayment_status
    amount_paid = np.empty(n_rows)
    days_overdue = np.empty(n_rows, dtype=int)
    payment_date = np.empty(n_rows, dtype=object)

    for status_value in REPAYMENT_STATUSES:
        mask = repayment_status == status_value
        n_mask = mask.sum()
        if n_mask == 0:
            continue

        if status_value == "ON_TIME":
            # Payé intégralement, à temps ou en avance -> 0 jour de retard
            amount_paid[mask] = amount_due[mask]
            days_overdue[mask] = 0
            early_offset = RNG.integers(-5, 1, size=n_mask)  # jusqu'à 5j d'avance
            payment_date[mask] = [
                d + timedelta(days=int(o))
                for d, o in zip(due_date[mask], early_offset)
            ]

        elif status_value == "LATE":
            # Payé intégralement mais en retard
            amount_paid[mask] = amount_due[mask]
            overdue = RNG.integers(1, 60, size=n_mask)
            days_overdue[mask] = overdue
            payment_date[mask] = [
                d + timedelta(days=int(o))
                for d, o in zip(due_date[mask], overdue)
            ]

        elif status_value == "DEFAULT":
            # Non remboursé (ou partiellement) -> pas de date de paiement
            partial_ratio = RNG.uniform(0, 0.5, size=n_mask)  # 0 à 50% remboursé
            amount_paid[mask] = (amount_due[mask] * partial_ratio).round(0)
            # Jours de retard = jours écoulés depuis l'échéance jusqu'à la fin de période
            days_overdue[mask] = [
                max(1, (end - d).days) for d in due_date[mask]
            ]
            payment_date[mask] = None  # NULL si impayé, conforme au schéma A.7

    df = pd.DataFrame({
        "repayment_id": [str(uuid.uuid4()) for _ in range(n_rows)],
        "timestamp": due_date,  # horodatage aligné sur l'échéance pour simplifier
        "loan_account_id": loan_account_id,
        "customer_id": customer_id,
        "country_code": country,
        "amount_due": amount_due,
        "amount_paid": amount_paid.round(0),
        "currency": CURRENCY_MAP[country],
        "due_date": due_date,
        "payment_date": payment_date,
        "days_overdue": days_overdue,
        "loan_type": loan_type,
        "repayment_status": repayment_status,
        "entity_type": RNG.choice(["BANK", "MICROFINANCE"], size=n_rows, p=[0.7, 0.3]),
    })

    return df