from datetime import datetime
from generators.referentials import generate_customers, generate_branches, generate_accounts
from generators.bank_transactions import generate_bank_transactions

customers = generate_customers(n=1000)
branches = generate_branches(n=200)
accounts = generate_accounts(customers, branches, n=1500)

txns = generate_bank_transactions(
    accounts, branches, country="CI", n_rows=500,
    start=datetime(2026, 1, 1), end=datetime(2026, 4, 1)
)

print(txns.head())
print(f"\n{len(txns)} transactions générées")

# Vérification cohérence référentielle
ci_accounts = set(accounts.loc[accounts.country_code == "CI", "account_id"])
orphans = ~txns["account_id"].isin(ci_accounts)
print(f"Transactions orphelines (account_id inconnu) : {orphans.sum()}")  # doit être 0