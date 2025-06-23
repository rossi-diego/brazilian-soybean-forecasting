import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from statsmodels.tsa.statespace.sarimax import SARIMAX

def walk_forward_forecast(y, exog, order, seasonal_order, n_test=6, verbose=True):
    history_y = y[:-n_test].copy()
    history_X = exog[:-n_test].copy()
    test_y = y[-n_test:]
    test_X = exog[-n_test:]

    predictions = []
    for i in range(n_test):
        exog_input = test_X.iloc[[i]]
        model = SARIMAX(
            history_y, exog=history_X,
            order=order, seasonal_order=seasonal_order,
            enforce_stationarity=False, enforce_invertibility=False
        )
        results = model.fit(disp=False, method='powell')
        pred = results.predict(start=len(history_y), end=len(history_y), exog=exog_input)
        predictions.append(pred.values[0])

        history_y = pd.concat([history_y, test_y.iloc[i:i+1]])
        history_X = pd.concat([history_X, test_X.iloc[i:i+1]])

    if verbose:
        test_y = y[-n_test:]
        y_true = test_y.values
        y_pred = np.array(predictions)
        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        bias = np.mean(y_pred - y_true)



    return y_true, y_pred, test_y.index

def print_model_evaluation(y_true, y_pred, y_naive=None, label="SARIMAX Model Forecast"):
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    bias = np.mean(y_pred - y_true)
    mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
    smape = 100 * np.mean(2 * np.abs(y_pred - y_true) / (np.abs(y_pred) + np.abs(y_true)))
    r2 = r2_score(y_true, y_pred)

    print(f"\n📉 Evaluation on last {len(y_true)} observations:\n")
    print(f"🔷 {label}:")
    print(f"MAE:   {mae:.2f}")
    print(f"RMSE:  {rmse:.2f}")
    print(f"Bias:  {bias:.2f}")
    print(f"MAPE:  {mape:.2f}%")
    print(f"SMAPE: {smape:.2f}%")
    print(f"R²:    {r2:.4f}")

    if y_naive is not None:
        mae_naive = mean_absolute_error(y_true, y_naive)
        rmse_naive = np.sqrt(mean_squared_error(y_true, y_naive))
        mape_naive = np.mean(np.abs((y_true - y_naive) / y_true)) * 100

        print(f"\n🔸 Naive Benchmark Forecast:")
        print(f"MAE:   {mae_naive:.2f}")
        print(f"RMSE:  {rmse_naive:.2f}")
        print(f"MAPE:  {mape_naive:.2f}%")

def print_forecast_summary(forecast, reference_series, label="Forecast"):
    last_real = reference_series.iloc[-1]
    mean_recent = reference_series.tail(12).mean()
    std_recent = reference_series.tail(12).std()

    change = forecast.values - last_real
    pct_change = (change / last_real) * 100

    print(f"\n🧪 Simulated Forecast Evaluation ({label}):")
    print(f"Last known value:       {last_real:.2f}")
    print(f"Mean of last 12 months: {mean_recent:.2f}")
    print(f"Forecast range:         {forecast.min():.2f} to {forecast.max():.2f}")
    print(f"Mean forecast change:   {pct_change.mean():+.2f}%")
    print(f"Forecast std vs hist:   {forecast.std():.2f} vs {std_recent:.2f}")

def simulate_exog_with_pct_trend(exog_df, n_periods, pct_map):
    """
    Simulate future exog by applying a total_pct over the full horizon, 
    distributed as compounded monthly growth.
    
    pct_map: dict of {col_name: total_pct_over_horizon}
    """
    last = exog_df.iloc[-1].copy()
    rows = []
    for step in range(1, n_periods + 1):
        new = last.copy()
        for col, total_pct in pct_map.items():
            if col in new:
                monthly_rate = (1 + total_pct)**(1/n_periods) - 1
                new[col] = last[col] * ((1 + monthly_rate) ** step)
        rows.append(new.copy())
    return pd.DataFrame(rows)