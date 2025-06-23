# Time Series Forecasting – Soybean Price

This project implements a complete forecasting pipeline for average brazilian Paranaguá soybean prices using SARIMAX models and scenario analysis with exogenous variables sourced from WASDE data.

---

## 📈 Model Results

- **Data Preparation**  
  - Initial dataset shape after dropping missing values: 178 rows × 28 columns  
  - After applying lagged exogenous features: 175 rows × 66 columns

- **Top Exogenous Features** (by Random Forest importance)  
  1. `corn_soy_price_interaction` (0.7437)  
  2. `corn_soy_price_interaction_lag1` (0.1699)  
  3. `corn_soy_price_interaction_lag2` (0.0098)  
  4. `exports_corn_cy_unitedstates` (0.0063)  
  5. `production_corn_oy_unitedstates` (0.0058)  
  6. `exports_soybean_oy_unitedstates` (0.0042)  
  7. `log_corn_quote` (0.0041)  
  8. `corn_quote` (0.0040)  
  9. `ending_soybean_oy_us_squared_lag3` (0.0039)  
  10. `production_soybean_oy_unitedstates` (0.0032)

- **Selected SARIMAX Order**  
  - Non-seasonal: (p,d,q) = (1, 0, 1)  
  - Seasonal: (P,D,Q,m) = (1, 0, 1, 12)

- **Model Evaluation** (last 12 observations)  
  - MAE: 5.96  
  - RMSE: 7.15  
  - Bias: 0.77  
  - MAPE: 1.46%  
  - SMAPE: 1.46%  
  - R²: 0.8677

- **Naive Benchmark**  
  - MAE: 54.06  
  - RMSE: 57.52  
  - MAPE: 13.59%

- **Scenario Forecast Evaluations**  
  - **Base Case**:  
    - Forecast range: 403.34 to 406.88  
    - Mean change vs last known: –2.02%  
    - Forecast std vs historical std: 1.25 vs 20.54  
  - **Optimistic Case**:  
    - Forecast range: 422.82 to 426.35  
    - Mean change vs last known: +2.70%  
    - Forecast std vs historical std: 1.25 vs 20.54  
  - **Pessimistic Case**:  
    - Forecast range: 383.46 to 385.58  
    - Mean change vs last known: –7.00%  
    - Forecast std vs historical std: 0.81 vs 20.54  


## 📁 Project Structure

```bash
├─ README_en.md            # Project documentation (this file)
├─ requirements.txt        # Python dependencies
├─ .env                    # Environment variables (e.g., WASDE_JWT)
├─ timeseries/             # Virtual environment (Python 3.11)
├─ src/
│  ├─ __init__.py
│  ├─ config.py            # Paths and project configuration
│  └─ model_utils.py       # Reusable functions (walk-forward, forecasting, etc.)
├─ main.py                 # Main script to run the forecasting pipeline
└─ notebooks/
   ├─ 01-wasde_exploration.ipynb  # WASDE data exploration and preprocessing
   ├─ 03-eda.ipynb                # Exploratory Data Analysis (decomposition, stationarity tests)
   └─ 04-sarimax.ipynb            # SARIMAX model fitting, validation, and metric definitions
```

---

## 🚀 Installation

1. **Clone the repository**  
   ```bash
   git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
   cd YOUR_REPO
   ```

2. **Create and activate the virtual environment** (Python 3.11)  
   ```bash
   python3.11 -m venv timeseries
   # macOS/Linux
   source timeseries/bin/activate
   # Windows PowerShell
   .\timeseries\Scripts\Activate.ps1
   ```

3. **Install dependencies**  
   ```bash
   python -m pip install --upgrade pip
   python -m pip install -r requirements.txt
   ```

4. **Configure environment variables**  
   - Create a `.env` file in the project root with:
     ```ini
     WASDE_JWT=<your_usda_jwt_token>
     ```
   - `config.py` automatically reads `os.getenv("WASDE_JWT")`.

---

## 🛠️ Usage

- **Notebooks**  
  - Open `notebooks/01-wasde_exploration.ipynb` to explore and preprocess WASDE data.  
  - Run `notebooks/03-eda.ipynb` for exploratory data analysis, including time series decomposition and stationarity tests.  
  - Use `notebooks/04-sarimax.ipynb` to fit the SARIMAX model, perform walk-forward validation, and review metric definitions.

- **Main Script**  
  ```bash
  python main.py
  ```

---

## ✨ Key Features

- Automated loading and cleaning of WASDE reports
- Feature engineering with lags and interactions
- Feature selection using Random Forest importance
- Automatic SARIMAX order identification via `pmdarima.auto_arima`
- Walk-forward validation for out-of-sample evaluation
- Scenario-based future forecasting with customizable percentage trends
- Both static and animated visualizations of forecasts

---

## 🤝 Contributing

Pull requests are welcome! Feel free to open issues or submit PRs for improvements.
