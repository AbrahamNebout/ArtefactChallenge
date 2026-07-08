"""
generators/mobile_money.py

Génère les paiements mobile money (mobile_money_[CC]_YYYYMMDD_NN.csv).
Particularité : receiver_country peut différer de sender_country
(transferts transfrontaliers), et le payment_type CROSS_BORDER_TRANSFER
doit être cohérent avec ça.
"""
import numpy as np
import pandas as pd
import uuid
from datetime import datetime

from config import PAYMENT_TYPES, OPERATORS, MM_STATUSES, MM_STATUS_PROBS, CURRENCY_MAP

RNG = np.random.default_rng()

# Corridors transfrontaliers réalistes en zone UEMOA (pays limitrophes)
CROSS_BORDER_CORRIDORS = {
    "CI": ["BF", "ML", "GN"],
    "SN": ["ML", "GN"],
    "ML": ["CI", "SN", "BF", "GN"],
    "BF": ["CI", "ML", "TG", "BJ"],
    "GN": ["CI", "SN", "ML"],
    "TG": ["BF", "BJ"],
    "BJ": ["TG", "BF"],
    "GH": ["CI", "TG"],
}


def _random_timestamps(start, end, n):
    delta_seconds = int((end - start).total_seconds())
    offsets = RNG.integers(0, delta_seconds, size=n)
    return np.array([start + pd.Timedelta(seconds=int(o)) for o in offsets])


def generate_mobile_money(
    customers: pd.DataFrame,
    country: str,
    n_rows: int,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    country_customers = customers.loc[
        (customers.country_code == country) & (customers.entity_type == "MOBILE_MONEY"),
        "customer_id"
    ].values

    if len(country_customers) == 0:
        raise ValueError(f"Aucun client MOBILE_MONEY pour {country}")

    sender_id = RNG.choice(country_customers, size=n_rows)

    payment_type = RNG.choice(
        PAYMENT_TYPES, size=n_rows,
        p=[0.45, 0.25, 0.15, 0.10, 0.05]  # P2P = usage le + courant
    )
    is_cross_border = payment_type == "CROSS_BORDER_TRANSFER"

    # --- Destinataire : même pays sauf si transfert transfrontalier ---
    receiver_id = np.empty(n_rows, dtype=object)
    receiver_country = np.empty(n_rows, dtype=object)

    domestic_mask = ~is_cross_border
    receiver_id[domestic_mask] = RNG.choice(country_customers, size=domestic_mask.sum())
    receiver_country[domestic_mask] = country

    if is_cross_border.any():
        n_cb = is_cross_border.sum()
        target_countries = RNG.choice(
            CROSS_BORDER_CORRIDORS.get(country, [country]), size=n_cb
        )
        receiver_country[is_cross_border] = target_countries
        # Pour chaque pays cible tiré, on choisit un client réel de ce pays
        receiver_id[is_cross_border] = [
            RNG.choice(
                customers.loc[
                    (customers.country_code == c) & (customers.entity_type == "MOBILE_MONEY"),
                    "customer_id"
                ].values
            )
            for c in target_countries
        ]

    amount = np.maximum(100, RNG.lognormal(mean=9, sigma=1.2, size=n_rows)).round(0)
    status = RNG.choice(MM_STATUSES, size=n_rows, p=MM_STATUS_PROBS)
    fee_amount = np.where(status == "SUCCESS", (amount * 0.015).round(0), 0)

    df = pd.DataFrame({
        "payment_id": [str(uuid.uuid4()) for _ in range(n_rows)],
        "timestamp": _random_timestamps(start, end, n_rows),
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "sender_country": country,
        "receiver_country": receiver_country,
        "amount": amount,
        "currency": CURRENCY_MAP[country],
        "payment_type": payment_type,
        "operator": RNG.choice(OPERATORS, size=n_rows, p=[0.5, 0.3, 0.2]),
        "status": status,
        "fee_amount": fee_amount,
        "entity_type": "MOBILE_MONEY",
    })

    return df