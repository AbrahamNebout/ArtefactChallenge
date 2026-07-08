"""
app.py — Interface Streamlit de génération de données WABA Group.
"""
import io
import time
import uuid
from datetime import datetime, date, timedelta

import streamlit as st
import pandas as pd

from config import (
    COUNTRIES, COUNTRY_NAMES, ENTITY_TYPES, DEFAULT_VOLUMES, DATA_TYPE_LABELS,
    FILE_PREFIXES,
)
from minio_client import get_minio_client, upload_dataframe
from generators.referentials import (
    generate_customers, generate_branches, generate_products, generate_accounts,
)
from generators.bank_transactions import generate_bank_transactions
from generators.insurance_ops import generate_insurance_operations
from generators.mobile_money import generate_mobile_money
from generators.loan_repayments import generate_loan_repayments

st.set_page_config(page_title="WABA Group — Générateur de données", layout="wide")

# --- Session state : garde les référentiels générés en mémoire entre les runs ---
if "referentials" not in st.session_state:
    st.session_state.referentials = None
if "generation_log" not in st.session_state:
    st.session_state.generation_log = []
if "last_generated" not in st.session_state:
    st.session_state.last_generated = {} 


st.title("🏦 WABA Group — Générateur de Données Financières")
st.caption("Simulation de l'activité multi-pays du groupe (8 pays, 4 lignes métier)")

st.header("1️⃣ Référentiels")
st.info(
    "Les référentiels doivent être générés en premier. Ils sont partagés "
    "entre tous les pays et réutilisés pour toutes les données transactionnelles."
)

col1, col2, col3, col4 = st.columns(4)
with col1:
    n_customers = st.number_input("Nombre de clients", value=DEFAULT_VOLUMES["customers"],
                                    min_value=1000, step=1000)
with col2:
    n_accounts = st.number_input("Nombre de comptes", value=DEFAULT_VOLUMES["accounts"],
                                   min_value=1000, step=1000)
with col3:
    n_branches = st.number_input("Nombre d'agences", value=DEFAULT_VOLUMES["branches"],
                                   min_value=10, step=10)
with col4:
    n_products = st.number_input("Nombre de produits", value=DEFAULT_VOLUMES["products"],
                                   min_value=10, step=5)

if st.button("🔄 Générer les référentiels", type="primary"):
    with st.spinner("Génération en cours..."):
        customers = generate_customers(n=n_customers)
        branches = generate_branches(n=n_branches)
        products = generate_products(n=n_products)
        accounts = generate_accounts(customers, branches, n=n_accounts)

        st.session_state.referentials = {
            "customers": customers, "branches": branches,
            "products": products, "accounts": accounts,
        }
    st.success(f"✅ {len(customers)} clients, {len(accounts)} comptes, "
               f"{len(branches)} agences, {len(products)} produits générés.")

if st.session_state.referentials:
    with st.expander("📊 Aperçu des référentiels"):
        tab1, tab2, tab3, tab4 = st.tabs(["Customers", "Accounts", "Branches", "Products"])
        with tab1:
            st.dataframe(st.session_state.referentials["customers"].head(50))
        with tab2:
            st.dataframe(st.session_state.referentials["accounts"].head(50))
        with tab3:
            st.dataframe(st.session_state.referentials["branches"].head(50))
        with tab4:
            st.dataframe(st.session_state.referentials["products"].head(50))
if st.button("☁️ Envoyer les référentiels vers MinIO"):
        client = get_minio_client()
        with st.spinner("Upload vers MinIO..."):
            for name, df in st.session_state.referentials.items():
                key = upload_dataframe(client, df, f"{name}.csv", "shared", "referentials")
                st.write(f"→ `{key}` ({len(df)} lignes)")
        st.success("Référentiels envoyés dans raw-landing/shared/referentials/")

st.header("2️⃣ Données Transactionnelles")

if st.session_state.referentials is None:
    st.warning("⚠️ Génère d'abord les référentiels ci-dessus.")
else:
    colA, colB = st.columns(2)
    with colA:
        data_type = st.selectbox(
            "Type de données",
            options=["bank_transactions", "insurance_operations",
                     "mobile_money", "loan_repayments"],
            format_func=lambda x: DATA_TYPE_LABELS[x],
        )
        selected_countries = st.multiselect(
            "Pays / Entité",
            options=COUNTRIES,
            default=COUNTRIES,
            format_func=lambda c: f"{c} — {COUNTRY_NAMES[c]}",
        )
    with colB:
        n_rows = st.number_input(
            "Nombre de lignes (par pays)",
            value=DEFAULT_VOLUMES[data_type], min_value=100, step=100,
        )
        date_range = st.date_input(
            "Période simulée",
            value=(date.today() - timedelta(days=90), date.today()),
        )

    mode = st.radio("Mode de génération", ["One-time", "Continue"], horizontal=True)
    if mode == "Continue":
        st.info(
            "🔄 En mode Continue, les **4 types de données sont générés simultanément** "
            "à chaque cycle (pas seulement le type sélectionné ci-dessus, qui ne sert "
            "qu'au mode One-time et à l'aperçu)."
        )
        interval = st.slider("Intervalle entre chaque génération (secondes)",
                              min_value=10, max_value=300, value=60)
        n_rows_continuous = st.number_input(
            "Nombre de lignes par cycle (par type de donnée et par pays)",
            value=50, min_value=10, step=10,
            help="Volume généré à CHAQUE cycle — reste petit, ça tourne en boucle "
                 "toutes les quelques secondes/minutes, pas une génération unique.",
        )
        
def generate_one_file(data_type: str, country: str, n_rows: int, start_dt: datetime, end_dt: datetime):
    refs = st.session_state.referentials

    if data_type == "bank_transactions":
        df = generate_bank_transactions(refs["accounts"], refs["branches"],
                                         country, n_rows, start_dt, end_dt)
    elif data_type == "insurance_operations":
        df = generate_insurance_operations(refs["customers"], refs["accounts"],
                                            country, n_rows, start_dt, end_dt)
    elif data_type == "mobile_money":
        df = generate_mobile_money(refs["customers"], country, n_rows, start_dt, end_dt)
    elif data_type == "loan_repayments":
        df = generate_loan_repayments(refs["customers"], refs["accounts"],
                                       country, n_rows, start_dt, end_dt)
    else:
        raise ValueError(f"Type de données inconnu : {data_type}")

    return df


if st.session_state.referentials is not None:
    start_dt = datetime.combine(date_range[0], datetime.min.time())
    end_dt = datetime.combine(date_range[1], datetime.min.time())

    if mode == "One-time":
        if st.button("🚀 Générer et envoyer vers MinIO", type="primary"):
            client = get_minio_client()
            progress = st.progress(0)
            for i, country in enumerate(selected_countries):
                try:
                    df = generate_one_file(data_type, country, n_rows, start_dt, end_dt)

                    # --- Découpage par jour : un fichier CSV par date de transaction,
                    #     conforme à la nomenclature bank_txn_CC_YYYYMMDD_NN.csv
                    #     (réaliste : un CBS écrit ses transactions jour par jour,
                    #     pas en un seul bloc rétroactif sur toute la période) ---
                    df["_day"] = pd.to_datetime(df["timestamp"]).dt.date
                    n_days = df["_day"].nunique()

                    for day, day_df in df.groupby("_day"):
                        day_df = day_df.drop(columns="_day")
                        day_str = day.strftime("%Y%m%d")
                        filename = f"{day_str}/{FILE_PREFIXES[data_type]}_{country}_{day_str}_01.csv"
                        key = upload_dataframe(client, day_df, filename, country, data_type)
                        st.session_state.generation_log.append(
                            f"✅ {key} — {len(day_df)} lignes — {datetime.now().strftime('%H:%M:%S')}"
                        )

                    st.session_state.last_generated[(data_type, country)] = df.drop(columns="_day")
                    st.caption(f"→ {country} : {n_days} fichiers générés (1 par jour)")
                except ValueError as e:
                    st.session_state.generation_log.append(f"❌ {country} : {e}")
                progress.progress((i + 1) / len(selected_countries))
            st.success("Génération terminée.")

    else:  # mode == "Continue"
        ALL_DATA_TYPES = ["bank_transactions", "insurance_operations",
                           "mobile_money", "loan_repayments"]

        run_continuous = st.toggle("▶️ Démarrer la génération continue")
        placeholder = st.empty()
        if run_continuous:
            client = get_minio_client()
            while run_continuous:
                # Fenêtre de temps "live" : les timestamps générés à ce cycle
                # couvrent les `interval` dernières secondes, pas la période
                # (potentiellement vieille de plusieurs mois) choisie dans le
                # date_input ci-dessus -> réaliste pour simuler un flux temps
                # réel consommé par NiFi/Kafka (Level 3).
                cycle_end = datetime.now()
                cycle_start = cycle_end - timedelta(seconds=interval)

                for dt in ALL_DATA_TYPES:
                    for country in selected_countries:
                        try:
                            df = generate_one_file(dt, country, n_rows_continuous,
                                                    cycle_start, cycle_end)

                            # Nom de fichier réellement déterminé ici (fin du bug
                            # où filename valait None) : jour + heure:minute:seconde
                            # pour rester unique à chaque cycle.
                            day_str = cycle_end.strftime("%Y%m%d")
                            time_str = cycle_end.strftime("%H%M%S")
                            filename = f"{day_str}/{FILE_PREFIXES[dt]}_{country}_{day_str}_{time_str}.csv"

                            key = upload_dataframe(client, df, filename, country, dt)
                            st.session_state.generation_log.append(
                                f"✅ {key} — {len(df)} lignes — {datetime.now().strftime('%H:%M:%S')}"
                            )
                            st.session_state.last_generated[(dt, country)] = df
                        except ValueError as e:
                            st.session_state.generation_log.append(f"❌ {dt}/{country} : {e}")

                with placeholder.container():
                    st.write(f"Dernière génération : {datetime.now().strftime('%H:%M:%S')}")
                    st.caption(
                        f"{len(ALL_DATA_TYPES)} types × {len(selected_countries)} pays générés. "
                        f"Prochaine génération dans {interval}s..."
                    )

                time.sleep(interval)


# --- Aperçu des données transactionnelles générées ---
if st.session_state.last_generated:
    with st.expander("📊 Aperçu des données transactionnelles générées", expanded=True):
        # On ne garde que les entrées correspondant au type actuellement sélectionné,
        # pour ne pas mélanger l'aperçu de bank_transactions avec mobile_money par ex.
        matching_keys = [
            (dt, c) for (dt, c) in st.session_state.last_generated.keys()
            if dt == data_type
        ]

        if not matching_keys:
            st.caption(
                f"Aucune donnée '{DATA_TYPE_LABELS[data_type]}' générée pour l'instant. "
                f"Lance une génération ci-dessus pour voir un aperçu ici."
            )
        else:
            country_tabs = st.tabs([c for (_, c) in matching_keys])
            for tab, (dt, c) in zip(country_tabs, matching_keys):
                with tab:
                    df_preview = st.session_state.last_generated[(dt, c)]
                    st.dataframe(df_preview.head(50))
                    st.caption(f"{len(df_preview)} lignes générées au total pour {c}.")

# --- Journal des générations ---
st.header("3️⃣ Journal")
# --- Journal des générations ---
if st.session_state.generation_log:
    st.code("\n".join(st.session_state.generation_log[-20:]), language=None)
else:
    st.caption("Aucune génération effectuée pour l'instant.")