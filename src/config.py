from pathlib import Path
from dotenv import load_dotenv
import os

# Load environment variables from .env file at project root
load_dotenv()

# Project folders
PROJECT_FOLDER = Path(__file__).resolve().parents[1]
DATA_FOLDER = PROJECT_FOLDER / "data"

# Data files
RAW_DATA = DATA_FOLDER / "raw_data"
PROCESSED_DATA = DATA_FOLDER / "processed_data"
WASDE_FOLDER = DATA_FOLDER / "wasde_files"

# Commodity files
WHEAT = PROCESSED_DATA / "wheat.xlsx"
WHEAT_CURRENT = PROCESSED_DATA / "wheat_current.xlsx"
WHEAT_NEXT = PROCESSED_DATA / "wheat_next.xlsx"
WHEAT_OUTLOOK = PROCESSED_DATA / "wheat_outlook.xlsx"
CORN = PROCESSED_DATA / "corn.xlsx"
CORN_CURRENT = PROCESSED_DATA / "corn_current.xlsx"
CORN_NEXT = PROCESSED_DATA / "corn_next.xlsx"
CORN_OUTLOOK = PROCESSED_DATA / "corn_outlook.xlsx"
SOYBEAN = PROCESSED_DATA / "soybean.xlsx"
SOYBEAN_CURRENT = PROCESSED_DATA / "soybean_current.xlsx"
SOYBEAN_NEXT = PROCESSED_DATA / "soybean_next.xlsx"
SOYBEAN_OUTLOOK = PROCESSED_DATA / "soybean_outlook.xlsx"
SOYBEAN_OIL = PROCESSED_DATA / "soybean_oil.xlsx"
SOYBEAN_OIL_CURRENT = PROCESSED_DATA / "soybean_oil_current.xlsx"
SOYBEAN_OIL_NEXT = PROCESSED_DATA / "soybean_oil_next.xlsx"
SOYBEAN_OIL_OUTLOOK = PROCESSED_DATA / "soybean_oil_outlook.xlsx"
SOYBEAN_MEAL = PROCESSED_DATA / "soybean_meal.xlsx"
SOYBEAN_MEAL_CURRENT = PROCESSED_DATA / "soybean_meal_current.xlsx"
SOYBEAN_MEAL_NEXT = PROCESSED_DATA / "soybean_meal_next.xlsx"
SOYBEAN_MEAL_OUTLOOK = PROCESSED_DATA / "soybean_meal_outlook.xlsx"
COMMODITY = PROCESSED_DATA / "commodity.xlsx"

# Model files
MODEL_SOYBEAN = PROCESSED_DATA / "model_soybean.xlsx"

# Quotes files
CORN_QUOTES = PROCESSED_DATA / "corn_quotes.xlsx"
SOYBEAN_QUOTES = PROCESSED_DATA / "soybean_quotes.xlsx"
SOYBEAN_PREMIUM_QUOTES = PROCESSED_DATA / "soybean_premium_quotes.xlsx"

# Image Files
FORECAST_SCENARIOS = DATA_FOLDER / "images" / "forecast_scenarios_animated.gif"

# WASDE Token (used for Cornell USDA API access)
WASDE_JWT = os.getenv("WASDE_JWT")

# Optional: raise an error if token is missing
if WASDE_JWT is None:
    raise EnvironmentError("Missing WASDE_JWT in your .env file")
