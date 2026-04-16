"""
MarketPulse Data Platform — Configuration
Copy this file to settings.py and fill in your credentials.
"""

# -----------------------------------------------
# Snowflake Connection
# -----------------------------------------------
SNOWFLAKE_ACCOUNT = "your_account.region"
SNOWFLAKE_USER = "your_user"
SNOWFLAKE_PASSWORD = "your_password"  # Use env vars in production
SNOWFLAKE_DATABASE = "MARKETPULSE"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"

# -----------------------------------------------
# API Keys
# -----------------------------------------------
# CoinGecko — free tier (no key needed for demo, but rate-limited)
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"

# Alternative.me Fear & Greed Index — no key needed
FEAR_GREED_URL = "https://api.alternative.me/fng/"

# -----------------------------------------------
# Pipeline Config
# -----------------------------------------------
# Top assets to track (CoinGecko IDs)
TRACKED_ASSETS = [
    "bitcoin",
    "ethereum",
    "solana",
    "cardano",
    "avalanche-2",
    "chainlink",
    "polkadot",
    "polygon-ecosystem-token",
    "uniswap",
    "aave",
]

# Number of days to backfill on first run
INITIAL_BACKFILL_DAYS = 365

# Spark config
SPARK_MASTER = "local[*]"
SPARK_APP_NAME = "MarketPulse"
