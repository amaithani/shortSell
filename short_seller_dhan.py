import pandas as pd
import requests
import time
from datetime import datetime, timedelta, date
import pytz

# Credentials
CLIENT_ID = "YOUR_CLIENT_ID"
ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"

# 📥 Load your uploaded Nifty 500 list
def load_nifty500_symbols():
    nifty500 = pd.read_csv("ind_nifty500list.csv")

    # Fetch the Dhan instrument list
    try:
        dhan_instruments_url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        dhan_instruments = pd.read_csv(dhan_instruments_url)
    except Exception as e:
        print(f"Error fetching Dhan instrument list: {e}")
        return {}

    # Filter for NSE Equity symbols
    dhan_nse_equity = dhan_instruments[(dhan_instruments['SEM_EXM_EXCH_ID'] == 'NSE') & (dhan_instruments['SEM_INSTRUMENT'] == 'E')].copy()

    # Create a mapping from symbol to security ID
    dhan_nse_equity['Symbol'] = dhan_nse_equity['SEM_TRADING_SYMBOL'].str.replace('-EQ', '')
    symbol_to_security_id = dhan_nse_equity.set_index('Symbol')['SEM_SECURITY_ID'].to_dict()

    # Map the Nifty 500 symbols to their security IDs
    nifty500['Dhan Symbol'] = nifty500['Symbol'].map(symbol_to_security_id)

    # Drop rows where a security ID could not be found
    nifty500.dropna(subset=['Dhan Symbol'], inplace=True)

    return nifty500[['Symbol', 'Dhan Symbol']].set_index('Symbol')['Dhan Symbol'].to_dict()


# 📊 Camarilla calculation
def calculate_camarilla_levels(high, low, close):
    range_ = high - low
    return {
        "L1": close - (range_ * 1.1 / 12),
        "H1": close + (range_ * 1.1 / 12),
        "L4": close - (range_ * 1.1 / 2),
        "H4": close + (range_ * 1.1 / 2),
        "L5": close - ((high / low) * close - close),
        "H5": (high / low) * close,
    }


# 📈 Fetch historical data from Dhan
def get_history(security_id, from_date, to_date, resolution="D", exchange_segment="NSE_EQ", instrument_type="EQUITY"):
    """
    Fetches historical data from the Dhan API.
    """
    if resolution == "D":
        url = "https://api.dhan.co/v2/charts/historical"
        payload = {
            "securityId": security_id,
            "exchangeSegment": exchange_segment,
            "instrument": instrument_type,
            "fromDate": from_date,
            "toDate": to_date
        }
    else:
        url = "https://api.dhan.co/v2/charts/intraday"
        payload = {
            "securityId": security_id,
            "exchangeSegment": exchange_segment,
            "instrument": instrument_type,
            "interval": resolution,
            "fromDate": f"{from_date} 09:15:00",
            "toDate": f"{to_date} 15:30:00"
        }

    headers = {
        "access-token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s').dt.tz_localize(pytz.utc)
        ist = pytz.timezone('Asia/Kolkata')
        df['timestamp'] = df['timestamp'].dt.tz_convert(ist)
        df["date"] = df["timestamp"].dt.date
        df["time"] = df["timestamp"].dt.time
        return df
    else:
        print(f"Error fetching data for {security_id}: {response.text}")
        return None


# 🔍 Analyze one symbol
def analyze_symbol(symbol, security_id, today):
    try:
        # Step 1: Daily data
        from_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        to_date = today.strftime("%Y-%m-%d")
        daily_df = get_history(security_id, from_date, to_date, resolution="D")

        if daily_df is None or len(daily_df) < 2:
            return None

        prev_day = daily_df.iloc[-2]
        camarilla = calculate_camarilla_levels(prev_day["high"], prev_day["low"], prev_day["close"])

        # Step 2: Intraday 15-min data for today
        intraday_df = get_history(security_id, today.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"), resolution="15")

        if intraday_df is None or intraday_df.empty:
            return None

        # Get 09:15 candle
        first_candle = intraday_df[intraday_df["time"] == datetime.strptime("09:15", "%H:%M").time()]
        if first_candle.empty:
            return None

        row = first_candle.iloc[0]
        support_broken = row["high"] >= camarilla["H1"] and row["close"] < camarilla["L1"]
        return {
            "symbol": symbol,
            "prev_high": prev_day["high"],
            "prev_low": prev_day["low"],
            "prev_close": prev_day["close"],
            "first_high": row["high"],
            "first_close": row["close"],
            **camarilla,
            "support_broken": support_broken
        }
    except Exception as e:
        print(f"Error for {symbol}: {e}")
        return None


# 🧠 Main loop
def run_camarilla_scan():
    today = date.today()

    symbols_map = load_nifty500_symbols()

    results = []
    for symbol, security_id in symbols_map.items():
        result = analyze_symbol(symbol, security_id, today)
        if result:
            results.append(result)
        time.sleep(0.02)  # Sleep to avoid rate limiting

    df = pd.DataFrame(results)

    support_broken_df = df[df["support_broken"] == True]

    # Save both full and filtered results
    df.to_csv("nifty500_camarilla_results.csv", index=False)
    support_broken_df.to_csv("support_broken_stocks.csv", index=False)

    #Print support-broken stock symbols
    if not support_broken_df.empty:
        print("Stocks where support is broken in first 15-min candle:")
        for sym in support_broken_df["symbol"]:
            print(f"{sym}")
    else:
        print("No stocks triggered support-broken condition today.")

# ▶️ Run
if __name__ == "__main__":
    run_camarilla_scan()
