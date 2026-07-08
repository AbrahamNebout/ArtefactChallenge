"""
generators/bank_transactions.py

Génère les transactions bancaires (bank_txn_[CC]_YYYYMMDD_NN.csv).
Chaque transaction DOIT référencer un account_id et un branch_id existants
dans les référentiels (contrainte de cohérence référentielle).
"""
import numpy as np
import pandas as pd
import uuid
from datetime import datetime, timedelta

from config import (
    TXN_TYPES, TXN_TYPE_PROBS, TXN_STATUSES, TXN_STATUS_PROBS,
    CHANNELS, CHANNEL_PROBS, CURRENCY_MAP,
)

RNG = np.random.default_rng()


def _random_timestamps(start: datetime, end: datetime, n: int) -> np.ndarray:
    """Tire n timestamps aléatoires uniformément entre start et end."""
    delta_seconds = int((end - start).total_seconds())
    offsets = RNG.integers(0, delta_seconds, size=n)
    return np.array([start + timedelta(seconds=int(o)) for o in offsets])


def generate_bank_transactions(
    accounts: pd.DataFrame,
    branches: pd.DataFrame,
    country: str,
    n_rows: int,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """
    Génère n_rows transactions bancaires pour UN pays donné.
    On génère pays par pays (plutôt que tout mélangé) car le fichier de sortie
    est nommé bank_txn_[CC]_YYYYMMDD_NN.csv -> partitionné par pays.
    """
    # 1. On restreint les comptes et agences à ce pays uniquement
    country_accounts = accounts.loc[accounts.country_code == country, "account_id"].values
    country_branches = branches.loc[branches.country_code == country, "branch_id"].values

    if len(country_accounts) == 0 or len(country_branches) == 0:
        raise ValueError(f"Aucun compte/agence trouvé pour {country} — "
                          f"génère les référentiels d'abord.")

    # 2. Tirage des comptes émetteurs et bénéficiaires
    #    (un compte peut débiter/créditer, y compris lui-même dans de rares cas,
    #    on les filtre pour rester réaliste)
    account_id = RNG.choice(country_accounts, size=n_rows)
    beneficiary_account = RNG.choice(country_accounts, size=n_rows)
    # Là où par hasard account_id == beneficiary_account, on re-tire une fois
    same_mask = account_id == beneficiary_account
    if same_mask.any():
        beneficiary_account[same_mask] = RNG.choice(
            country_accounts, size=same_mask.sum()
        )

    branch_id = RNG.choice(country_branches, size=n_rows)
    txn_type = RNG.choice(TXN_TYPES, size=n_rows, p=TXN_TYPE_PROBS)
    status = RNG.choice(TXN_STATUSES, size=n_rows, p=TXN_STATUS_PROBS)
    channel = RNG.choice(CHANNELS, size=n_rows, p=CHANNEL_PROBS)

    # 3. Montants : distribution log-normale -> beaucoup de petites transactions,
    #    quelques grosses (réaliste pour des flux financiers, contrairement
    #    à une distribution uniforme qui donnerait des montants "plats" irréalistes)
    amount = np.maximum(500, RNG.lognormal(mean=12, sigma=1.5, size=n_rows)).round(0)

    # 4. Frais : uniquement si la transaction a réussi, sinon 0
    fee_amount = np.where(status == "SUCCESS", (amount * 0.001).round(0), 0)

    df = pd.DataFrame({
        "transaction_id": [str(uuid.uuid4()) for _ in range(n_rows)],
        "timestamp": _random_timestamps(start, end, n_rows),
        "account_id": account_id,
        "beneficiary_account": beneficiary_account,
        "branch_id": branch_id,
        "country_code": country,
        "transaction_type": txn_type,
        "amount": amount,
        "currency": CURRENCY_MAP[country],
        "channel": channel,
        "transaction_status": status,
        "fee_amount": fee_amount,
        "entity_type": "BANK",
    })

    return df