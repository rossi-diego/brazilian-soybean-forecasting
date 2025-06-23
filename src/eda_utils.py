# eda_utils.py
import pandas as pd

def preprocess_wasde_data(df_wasde):
    """
    Preprocesses WASDE report data by ensuring proper datetime formatting and sorting.
    
    Parameters:
    -----------
    df_wasde : pd.DataFrame
        Raw WASDE data with 'report_date' column
        
    Returns:
    --------
    pd.DataFrame
        Processed WASDE data with datetime index
    """
    df = df_wasde.copy()
    df["report_date"] = pd.to_datetime(df["report_date"])
    return df.sort_values("report_date").reset_index(drop=True)

def preprocess_futures_data(df_quotes):
    """
    Preprocesses futures quotes data, calculating soybean composite price.
    
    Parameters:
    -----------
    df_quotes : pd.DataFrame
        Raw futures data with soybean/corn quotes
        
    Returns:
    --------
    pd.DataFrame
        Processed data with composite soybean price and datetime index
    """
    df = df_quotes.copy()
    # Calculate soybean composite price (conversion to USD/MT)
    df['soybean'] = ((df['soybean_quote'] + df['soybean_premium']) / 100) * 36.7454
    df = df.drop(columns=["soybean_quote", "soybean_premium"])
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)

def aggregate_prices_by_report_window(df_wasde, df_quotes):
    """
    Aggregates futures prices between consecutive WASDE report dates.
    
    Parameters:
    -----------
    df_wasde : pd.DataFrame
        Preprocessed WASDE report data
    df_quotes : pd.DataFrame
        Preprocessed futures quotes data
        
    Returns:
    --------
    pd.DataFrame
        Modeling-ready DataFrame with WASDE features and aggregated prices
    """
    records = []
    
    for i in range(1, len(df_wasde)):
        current_report = df_wasde.iloc[i]
        prev_report_date = df_wasde.iloc[i-1]["report_date"]
        window_end = current_report["report_date"] - pd.Timedelta(days=1)
        
        # Filter quotes in the reporting window
        mask = (df_quotes["date"] >= prev_report_date) & (df_quotes["date"] <= window_end)
        window_quotes = df_quotes.loc[mask]
        
        # Create merged record
        record = current_report.to_dict()
        record.update({
            "corn_quote": window_quotes["corn_quote"].mean(),
            "soybean": window_quotes["soybean"].mean(),
            "window_start": prev_report_date,
            "window_end": window_end
        })
        records.append(record)
    
    return pd.DataFrame(records)

# Função principal que orquestra todo o processo
def prepare_modeling_data(raw_wasde, raw_quotes):
    """
    Full pipeline to transform raw WASDE and futures data into modeling-ready format.
    
    Parameters:
    -----------
    raw_wasde : pd.DataFrame
        Raw WASDE report data
    raw_quotes : pd.DataFrame
        Raw futures market data
        
    Returns:
    --------
    pd.DataFrame
        Final modeling dataset
    """
    df_wasde = preprocess_wasde_data(raw_wasde)
    df_quotes = preprocess_futures_data(raw_quotes)
    return aggregate_prices_by_report_window(df_wasde, df_quotes)

def test_lagged_correlation(df, target_col='soybean', date_col='report_date', max_lag=6):
    df = df.sort_values(date_col).reset_index(drop=True)
    df[date_col] = pd.to_datetime(df[date_col])
    exog_cols = [col for col in df.columns if col not in [target_col, date_col]]

    rows = []
    for col in exog_cols:
        for lag in range(1, max_lag + 1):
            shifted = df[col].shift(lag)
            corr = df[target_col].corr(shifted)
            rows.append({
                'Variable': col,
                'Lag (months)': lag,
                'Correlation with Target': corr
            })

    corr_df = pd.DataFrame(rows)
    return corr_df.pivot(index='Variable', columns='Lag (months)', values='Correlation with Target')


def top_lagged_predictors(pivot_table, top_n=10):
    best_lags = (
        pivot_table.abs()
        .stack()
        .reset_index()
        .rename(columns={0: 'Absolute Correlation'})
    )

    best_lags['Signed Correlation'] = [
        pivot_table.loc[row['Variable'], row['Lag (months)']] for _, row in best_lags.iterrows()
    ]

    best_lags = best_lags.sort_values('Absolute Correlation', ascending=False).drop_duplicates('Variable')

    return best_lags[['Variable', 'Lag (months)', 'Signed Correlation', 'Absolute Correlation']].head(top_n)