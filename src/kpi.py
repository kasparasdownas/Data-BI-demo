import pandas as pd

def kpi_growth_qoq(df: pd.DataFrame, value_col: str, date_col: str) -> float:
    s = (
        df.sort_values(date_col)
          .set_index(date_col)[value_col]
          .resample("QE") 
          .sum()
    )
    if len(s) < 2:
        return 0.0
    prev, curr = s.iloc[-2], s.iloc[-1]
    return float((curr - prev) / prev * 100) if prev else 0.0

def kpi_churn(rate_series: pd.Series) -> float:
    return float(rate_series.mean() * 100)

def kpi_arpu(revenue: float, users: float) -> float:
    return float(revenue / users) if users else 0.0
