import numpy as np
import pandas as pd
from datetime import date

from config import (
    COUNTRIES, REGIONS, ENTITY_TYPES, SEGMENTS, SEGMENT_PROBS,
    KYC_LEVELS, KYC_PROBS, ACCOUNT_TYPES, ACCOUNT_STATUSES, BRANCH_TYPES,
)

RNG = np.random.default_rng()


def generate_customers(n: int = 500_000) -> pd.DataFrame:

    countries = RNG.choice(COUNTRIES, size=n)
    entities = RNG.choice(ENTITY_TYPES, size=n, p=[0.55, 0.20, 0.18, 0.07])
    segments = RNG.choice(SEGMENTS, size=n, p=SEGMENT_PROBS)
    kyc = RNG.choice(KYC_LEVELS, size=n, p=KYC_PROBS)

    start, end = date(2010, 1, 1), date(2025, 12, 31)
    offsets = RNG.integers(0, (end - start).days, size=n)
    onboarding = [start + pd.Timedelta(days=int(o)) for o in offsets]

    regions = [RNG.choice(REGIONS[c]) for c in countries]
    is_active = RNG.random(n) > 0.08

    df = pd.DataFrame({
        "country_code": countries,
        "entity_type": entities,
        "segment": segments,
        "kyc_level": kyc,
        "onboarding_date": onboarding,
        "region": regions,
        "is_active": is_active,
    })


    seq = df.groupby("country_code").cumcount() + 1
    df.insert(0, "customer_id",
              df["country_code"] + "-C-" + seq.astype(str).str.zfill(6))
    df["customer_id"] = "WABA-" + df["customer_id"]

    return df


def generate_branches(n: int = 200) -> pd.DataFrame:
    countries = RNG.choice(COUNTRIES, size=n)
    entities = RNG.choice(ENTITY_TYPES, size=n, p=[0.55, 0.20, 0.18, 0.07])
    branch_types = RNG.choice(BRANCH_TYPES, size=n, p=[0.35, 0.25, 0.25, 0.15])
    cities = [RNG.choice(REGIONS[c]) for c in countries]
    is_active = RNG.random(n) > 0.05

    df = pd.DataFrame({
        "country_code": countries,
        "entity_type": entities,
        "city": cities,
        "region": cities,
        "branch_type": branch_types,
        "is_active": is_active,
    })

    seq = df.groupby("country_code").cumcount() + 1
    df.insert(0, "branch_id",
              "WABA-" + df["country_code"] + "-B-" + seq.astype(str).str.zfill(3))

    return df


def generate_products(n: int = 50) -> pd.DataFrame:

    catalog = {
        "BANK": ["Compte Courant", "Livret Épargne", "Crédit Conso",
                 "Crédit Immobilier", "Crédit PME"],
        "INSURANCE": ["Assurance Vie", "Auto Tiers", "Auto Tous Risques",
                      "Habitation", "Santé Individuelle", "Prévoyance"],
        "MOBILE_MONEY": ["Portefeuille Standard", "Portefeuille Marchand",
                         "Transfert International"],
        "MICROFINANCE": ["Microcrédit Agricole", "Microcrédit Commerce",
                          "Tontine Digitale"],
    }
    rows = []
    pid = 1
    while len(rows) < n:
        entity = RNG.choice(list(catalog.keys()))
        name = RNG.choice(catalog[entity])
        rows.append({
            "product_id": f"WABA-P-{pid:04d}",
            "entity_type": entity,
            "product_name": name,
            "is_active": bool(RNG.random() > 0.1),
        })
        pid += 1
    return pd.DataFrame(rows)


def generate_accounts(customers: pd.DataFrame, branches: pd.DataFrame,
                       n: int = 800_000) -> pd.DataFrame:

    rows = []
    for country in customers["country_code"].unique():
        cust_ids = customers.loc[customers.country_code == country, "customer_id"].values
        branch_ids = branches.loc[branches.country_code == country, "branch_id"].values
        if len(branch_ids) == 0:
            continue 

        share = len(cust_ids) / len(customers)
        n_country = max(1, int(n * share))

        rows.append(pd.DataFrame({
            "country_code": country,
            "customer_id": RNG.choice(cust_ids, size=n_country),
            "branch_id": RNG.choice(branch_ids, size=n_country),
            "account_type": RNG.choice(ACCOUNT_TYPES, size=n_country,
                                        p=[0.40, 0.25, 0.15, 0.15, 0.05]),
        }))

    df = pd.concat(rows, ignore_index=True)

    from config import CURRENCY_MAP
    df["currency"] = df["country_code"].map(CURRENCY_MAP)
    df["balance"] = RNG.lognormal(mean=13, sigma=1.5, size=len(df)).round(2)
    df["credit_limit"] = np.where(
        df["account_type"] == "CURRENT",
        RNG.choice([0, 100000, 500000, 1000000], size=len(df), p=[0.6, 0.2, 0.15, 0.05]),
        0.0,
    )
    start, end = date(2010, 1, 1), date(2025, 12, 31)
    offsets = RNG.integers(0, (end - start).days, size=len(df))
    df["opened_date"] = [start + pd.Timedelta(days=int(o)) for o in offsets]
    df["status"] = RNG.choice(ACCOUNT_STATUSES, size=len(df), p=[0.85, 0.05, 0.05, 0.05])

    seq = df.groupby("country_code").cumcount() + 1
    df.insert(0, "account_id",
              "WABA-" + df["country_code"] + "-A-" + seq.astype(str).str.zfill(7))

    return df