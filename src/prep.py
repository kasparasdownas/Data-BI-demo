from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import requests
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import matplotlib.dates as mdates

from .kpi import kpi_growth_qoq, kpi_churn, kpi_arpu

RAW = Path("data/raw.csv")
OUT = Path("data/clean.csv")

# ---------- Energi Data Service helpers ----------

def save_hourly_summary(df: pd.DataFrame, out_path: Path) -> None:
    if df.empty:
        return
    tmp = df.copy()
    tmp["hour"] = tmp["HourDK"].dt.hour
    grp = tmp.groupby("hour", as_index=False).agg(
        avg_price_dkk=("price_dkk", "mean"),
        avg_consumption_mwh=("consumption_mwh", "mean"),
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    grp.to_csv(out_path, index=False)

def get_completed_window(days: int = 7, lag_days: int = 8):
    end = datetime.now() - timedelta(days=lag_days)
    start = end - timedelta(days=days)
    return (
        start.replace(microsecond=0).isoformat(timespec="minutes"),
        end.replace(microsecond=0).isoformat(timespec="minutes"),
    )

def fetch_elspotprices(days: int = 7, price_area: str = "DK1") -> pd.DataFrame:
    start_iso, end_iso = get_completed_window(days=days, lag_days=8)
    url = "https://api.energidataservice.dk/dataset/Elspotprices"
    params = {
        "start": start_iso,
        "end": end_iso,
        "filter": json.dumps({"PriceArea": [price_area]}),
        "columns": "HourDK,PriceArea,SpotPriceDKK",
        "sort": "HourDK asc",
        "limit": 0,
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    recs = r.json().get("records", [])
    df = pd.DataFrame(recs)
    if df.empty:
        return df
    df["HourDK"] = pd.to_datetime(df["HourDK"])
    df = df.rename(columns={"SpotPriceDKK": "price_dkk"})
    return df[["HourDK", "price_dkk", "PriceArea"]]

def save_price_vs_consumption_plot(df: pd.DataFrame, out_path: Path) -> None:
    if df.empty:
        return

    df = df.sort_values("HourDK").copy()
    df["price_dkk"] = pd.to_numeric(df["price_dkk"], errors="coerce")
    df["consumption_mwh"] = pd.to_numeric(df["consumption_mwh"], errors="coerce")
    df = df.dropna(subset=["price_dkk", "consumption_mwh"])

    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax2 = ax1.twinx()

    ax1.plot(df["HourDK"], df["price_dkk"], color="tab:blue", label="Price (DKK/MWh)")
    ax2.plot(df["HourDK"], df["consumption_mwh"], color="tab:orange", label="Consumption (MWh)")

    ax1.set_xlabel("Time (hourly)")
    ax1.set_ylabel("Price (DKK/MWh)", color="tab:blue")
    ax2.set_ylabel("Consumption (MWh)", color="tab:orange")

    ax1.xaxis.set_major_locator(mdates.HourLocator(interval=6))   # tick every 6 hours
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    fig.autofmt_xdate(rotation=45)

    ax1.xaxis.set_minor_locator(mdates.HourLocator(interval=1))
    ax1.grid(which="minor", axis="x", linestyle=":", color="grey", alpha=0.5)

    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")

    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)

def fetch_consumption(days: int = 7, price_area: str = "DK1") -> pd.DataFrame:
    start_iso, end_iso = get_completed_window(days=days, lag_days=8)
    url = "https://api.energidataservice.dk/dataset/ProductionConsumptionSettlement"
    params_base = {
        "start": start_iso,
        "end": end_iso,
        "filter": json.dumps({"PriceArea": [price_area]}),
        "sort": "HourDK asc",  # bliver ignoreret hvis 'HourDK' ikke findes
        "limit": 2000,         # rigeligt til 7 dage i timesopløsning
    }

    # 1) prøv med alle kolonner (ingen 'columns' param)
    r = requests.get(url, params=params_base, timeout=60)
    try:
        r.raise_for_status()
    except Exception as e:
        # fallback: prøv uden sortering (hvis sort-feltet ikke findes)
        params_alt = {k: v for k, v in params_base.items() if k != "sort"}
        r = requests.get(url, params=params_alt, timeout=60)
        r.raise_for_status()

    recs = r.json().get("records", [])
    df = pd.DataFrame(recs)
    if df.empty:
        print("[Consumption] Tomt svar – tjek periode/dataset.")
        return df

    # 2) find tidskolonnen
    time_candidates = ["HourDK", "TimeDK", "HourUTC", "TimeUTC"]
    time_col = next((c for c in time_candidates if c in df.columns), None)
    if not time_col:
        print(f"[Consumption] Kunne ikke finde tidskolonne i: {list(df.columns)}")
        return pd.DataFrame()

    # 3) find en forbrugskolonne
    # prioriterer MWh-agtige felter, men tager hvad der ligner 'Consumption'
    cons_candidates = [c for c in df.columns if "consumption" in c.lower()]
    # sæt en blød prioritering
    pref_order = ["ConsumptionMWh", "TotalConsumptionMWh", "Consumption", "TotalCon", "Cons"]
    cons_col = None
    for p in pref_order:
        for c in cons_candidates:
            if c.lower() == p.lower():
                cons_col = c
                break
        if cons_col:
            break
    if not cons_col and cons_candidates:
        cons_col = cons_candidates[0]

    if not cons_col:
        print(f"[Consumption] Fandt ingen consumption-kolonne i: {list(df.columns)}")
        return pd.DataFrame()

    # 4) parse tid + returnér normaliserede navne
    df[time_col] = pd.to_datetime(df[time_col])
    out = df[[time_col, cons_col]].copy()
    out = out.rename(columns={time_col: "HourDK", cons_col: "consumption_mwh"})
    return out


# ---------- Data prep (demo data) ----------

def load() -> pd.DataFrame:
    if not RAW.exists():
        dates = pd.date_range("2024-01-01", "2025-08-31", freq="D")
        df = pd.DataFrame({
            "date": dates,
            "revenue": (100 + (pd.Series(range(len(dates))) * 0.35)).round(2),
            "users": (1000 + (pd.Series(range(len(dates))) * 0.6)).astype(int),
            "churn_rate": 0.04,   
        })
        return df
    return pd.read_csv(RAW, parse_dates=["date"])

def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["date", "revenue", "users"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    return df

# ---------- Hovedkørsel ----------

def main() -> None:
    # 1) Din eksisterende demo-datastrøm (fallback + KPI’er)
    df = clean(load())
    OUT.parent.mkdir(exist_ok=True)
    df.to_csv(OUT, index=False)

    growth = kpi_growth_qoq(df, "revenue", "date")
    churn = kpi_churn(df["churn_rate"]) if "churn_rate" in df.columns else 0.0
    arpu = kpi_arpu(df["revenue"].mean(), df["users"].mean())
    print(f"[DEMO] KPI growth QoQ: {growth:.2f}% | Churn: {churn:.2f}% | ARPU: {arpu:.2f}")

    # 2) Rigtige DK-timepriser (Elspotprices)
    try:
        prices = fetch_elspotprices(days=7, price_area="DK1")
        if not prices.empty:
            prices_path = Path("data/elspotprices_7d.csv")
            prices.to_csv(prices_path, index=False)

            mean_price = prices["price_dkk"].mean()
            spread = prices["price_dkk"].max() - prices["price_dkk"].min()
            max_row = prices.loc[prices["price_dkk"].idxmax()]
            print(
                f"[Elspotprices] Avg: {mean_price:.2f} DKK/MWh | "
                f"Max: {max_row['price_dkk']:.2f} @ {max_row['HourDK']} | "
                f"Spread: {spread:.2f}"
            )
        else:
            print("[Elspotprices] Ingen data modtaget (tjek periode/params).")
    except Exception as e:
        print(f"[Elspotprices] Fejl: {e}")

    # 3) Forbrug og join med pris
    try:
        cons = fetch_consumption(days=7, price_area="DK1")
        if 'prices' in locals() and not prices.empty and not cons.empty:
            dfp = prices[["HourDK", "price_dkk"]].copy()
            dfc = cons[["HourDK", "consumption_mwh"]].copy()
            combo = pd.merge(dfp, dfc, on="HourDK", how="inner")
            combo["est_cost_dkk"] = combo["price_dkk"] * combo["consumption_mwh"]
            combo_path = Path("data/price_consumption_7d.csv")
            combo.to_csv(combo_path, index=False)
            save_hourly_summary(combo, Path("report/hourly_summary.csv"))
            save_price_vs_consumption_plot(combo, Path("report/price_vs_consumption.png"))
            

            avg_price = combo["price_dkk"].mean()
            peak = combo.loc[combo["consumption_mwh"].idxmax()]
            total_cost = combo["est_cost_dkk"].sum()

            print(
                f"[JOIN] Avg price: {avg_price:.2f} DKK/MWh | "
                f"Peak consumption: {peak['consumption_mwh']:.1f} MWh @ {peak['HourDK']} | "
                f"Est. total cost (7d): {total_cost:,.0f} DKK"
            )
        else:
            if 'cons' in locals() and cons.empty:
                print("[JOIN] Forbrug tomt; skipper join.")
    except Exception as e:
        print(f"[JOIN] Fejl: {e}")

if __name__ == "__main__":
    main()