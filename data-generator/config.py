
# --- Pays & zones monétaires ---
COUNTRIES = ['CI', 'SN', 'ML', 'BF', 'GN', 'TG', 'BJ', 'GH']
UEMOA = ['CI', 'SN', 'ML', 'BF', 'GN', 'TG', 'BJ']  # zone XOF
CURRENCY_MAP = {c: 'XOF' for c in UEMOA} | {'GH': 'GHS'}

COUNTRY_NAMES = {
    'CI': 'Côte d\'Ivoire', 'SN': 'Sénégal', 'ML': 'Mali',
    'BF': 'Burkina Faso', 'GN': 'Guinée', 'TG': 'Togo',
    'BJ': 'Bénin', 'GH': 'Ghana',
}

REGIONS = {
    'CI': ['Abidjan', 'Bouake', 'Yamoussoukro', 'San Pedro', 'Korhogo'],
    'SN': ['Dakar', 'Thies', 'Ziguinchor', 'Saint-Louis', 'Kaolack'],
    'ML': ['Bamako', 'Sikasso', 'Segou', 'Mopti', 'Tombouctou'],
    'BF': ['Ouagadougou', 'Bobo-Dioulasso', 'Koudougou', 'Banfora'],
    'GN': ['Conakry', 'Nzerekore', 'Kindia', 'Kankan'],
    'TG': ['Lome', 'Sokode', 'Kara', 'Atakpame'],
    'BJ': ['Cotonou', 'Porto-Novo', 'Parakou', 'Abomey-Calavi'],
    'GH': ['Accra', 'Kumasi', 'Tamale', 'Cape Coast', 'Sunyani'],
}

# --- Entités métier ---
ENTITY_TYPES = ['BANK', 'INSURANCE', 'MOBILE_MONEY', 'MICROFINANCE']

# --- Référentiels ---
SEGMENTS = ['RETAIL', 'SME', 'CORPORATE', 'PREMIUM']
SEGMENT_PROBS = [0.65, 0.20, 0.10, 0.05]
KYC_LEVELS = ['BASIC', 'STANDARD', 'ENHANCED']
KYC_PROBS = [0.3, 0.5, 0.2]

ACCOUNT_TYPES = ['CURRENT', 'SAVINGS', 'LOAN', 'MOBILE_WALLET', 'INSURANCE_POLICY']
ACCOUNT_STATUSES = ['ACTIVE', 'FROZEN', 'CLOSED', 'DORMANT']

BRANCH_TYPES = ['FULL_SERVICE', 'DIGITAL_ONLY', 'AGENCY_BANKING', 'ATM_POINT']

# --- Transactions bancaires ---
TXN_TYPES = ['TRANSFER', 'PAYMENT', 'WITHDRAWAL', 'DEPOSIT', 'INTERNATIONAL_WIRE']
TXN_TYPE_PROBS = [0.35, 0.30, 0.15, 0.15, 0.05]
TXN_STATUSES = ['SUCCESS', 'FAILED', 'REVERSED']
TXN_STATUS_PROBS = [0.92, 0.05, 0.03]
CHANNELS = ['BRANCH', 'ATM', 'MOBILE_APP', 'INTERNET_BANKING', 'USSD']
CHANNEL_PROBS = [0.20, 0.15, 0.35, 0.20, 0.10]

# --- Assurance ---
OPERATION_TYPES = ['PREMIUM_PAYMENT', 'CLAIM_SUBMISSION', 'CLAIM_PAYMENT',
                    'POLICY_RENEWAL', 'POLICY_CANCELLATION']
PRODUCT_LINES = ['VIE', 'IARD_AUTO', 'IARD_HABITATION', 'IARD_SANTE', 'PREVOYANCE']
CLAIM_STATUSES = ['PENDING', 'APPROVED', 'REJECTED', 'PAID']

# --- Mobile Money ---
PAYMENT_TYPES = ['P2P', 'MERCHANT_PAYMENT', 'BILL_PAYMENT', 'AIRTIME', 'CROSS_BORDER_TRANSFER']
OPERATORS = ['WABA_PAY', 'ORANGE_MONEY_PARTNER', 'MTN_PARTNER']
MM_STATUSES = ['SUCCESS', 'FAILED', 'PENDING']
MM_STATUS_PROBS = [0.94, 0.04, 0.02]

# --- Prêts / Microfinance ---
LOAN_TYPES = ['CONSUMER', 'MORTGAGE', 'SME', 'AGRICULTURAL', 'MICROCREDIT']
REPAYMENT_STATUSES = ['ON_TIME', 'LATE', 'DEFAULT']
REPAYMENT_STATUS_PROBS = [0.82, 0.12, 0.06]

# --- Buckets MinIO ---
BUCKET_RAW_LANDING = 'raw-landing'
BUCKET_LAKEHOUSE = 'lakehouse'
BUCKET_ARCHIVE = 'archive'



# --- Volumes par défaut (cahier des charges 1.1) ---
DEFAULT_VOLUMES = {
    "customers": 500_000,
    "accounts": 800_000,
    "branches": 200,
    "products": 50,
    "bank_transactions": 10_000,
    "insurance_operations": 5_000,
    "mobile_money": 20_000,
    "loan_repayments": 5_000,  # non précisé explicitement, valeur raisonnable
}

DATA_TYPE_LABELS = {
    "bank_transactions": "Transactions bancaires",
    "insurance_operations": "Opérations d'assurance",
    "mobile_money": "Paiements mobile money",
    "loan_repayments": "Remboursements de crédit",
    "referentials": "Référentiels (customers, accounts, branches, products)",
}

FILE_PREFIXES = {
    "bank_transactions": "bank_txn",
    "insurance_operations": "insurance_ops",
    "mobile_money": "mobile_money",
    "loan_repayments": "loan_repayments",
}

