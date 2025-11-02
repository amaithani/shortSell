import time
from datetime import datetime, timedelta, date
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# FYI: Use your fyers client as fyers (fyers.history). Import/setup your fyers client separately.
# from fyers_apiv3 import fyersModel
# fyers = fyersModel.FyersModel(client_id=..., token=...)

# ---------- CONFIG ----------
SERVICE_ACCOUNT_FILE = "/Users/admin/Downloads/python_project/stocksdetailsalgosheet-74173c3a8b69.json"  # uploaded credentials. :contentReference[oaicite:1]{index=1}
SHEET_NAME = "SupportBreakers"   # change if you want another sheet name
CSV_SYMBOLS = "ind_nifty500list.csv"
MAX_LOSERS = 50

# ---------- Google Sheets auth ----------
def gsheets_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc

# ---------- Camarilla with extended levels (L1..L5, H1..H5) ----------
def calculate_camarilla_levels(high, low, close):
    rng = high - low
    # Classic Camarilla core levels (extended)
    L1 = close - (rng * 1.1 / 12)
    L2 = close - (rng * 1.1 / 6)
    L3 = close - (rng * 1.1 / 4)
    L4 = close - (rng * 1.1 / 2)
    L5 = close - ((high / low) * close - close)
    H1 = close + (rng * 1.1 / 12)
    H2 = close + (rng * 1.1 / 6)
    H3 = close + (rng * 1.1 / 4)
    H4 = close + (rng * 1.1 / 2)
    H5 = (high / low) * close
    return {
        "L1": L1, "L2": L2, "L3": L3, "L4": L4, "L5": L5,
        "H1": H1, "H2": H2, "H3": H3, "H4": H4, "H5": H5
    }

# ---------- Load symbols (Nifty 500 list) ----------
def load_symbols():
    df = pd.read_csv(CSV_SYMBOLS)
    df['fyers_symbol'] = "NSE:" + df['Symbol'] + "-" + df['Series']
    return df

# ---------- Helper: minimal progress print ----------
def progress(i, total):   
    if i % 25 == 0:
        print(f"Processed {i}/{total}", end="\r")

# ---------- Fetch wrapper (replace with your fyers client calls) ----------
def fetch_history(fyers, symbol, resolution, start_date, end_date):
    """
    Wrapper to call fyers.history. Must return dict like { "s":"ok", "candles": [...] }
    This function should call your fyers.history or equivalent.
    """
    payload = {
        "symbol": symbol,
        "resolution": resolution,
        "date_format": "1",
        #"range_from": start_date.strftime("%Y-%m-%d"),
        #"range_to": end_date.strftime("%Y-%m-%d"),
        "range_from": start_date,
        "range_to": end_date,
        "cont_flag": "1"
    }
    response = fyers.history(payload)
    if response.get("s") != "ok" or "candles" not in response:
        return
    df = pd.DataFrame(response["candles"], columns=["timestamp", "open", "high", "low", "close", "volume"])
    # Convert Timestamp to datetime in UTC
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s").dt.tz_localize(pytz.utc)

     # Convert Timestamp to IST
    ist = pytz.timezone('Asia/Kolkata')
    df['timestamp'] = df['timestamp'].dt.tz_convert(ist)
    
    df["date"] = df["timestamp"].dt.date
    df["time"] = df["timestamp"].dt.time
    return df
    #return fyers.history(payload)  # expects your fyers client object available in scope

# ---------- Step 1: top N losers based on first 5-min candle (09:15 close) ----------
def get_top_n_losers_first5(fyers, symbols, n=MAX_LOSERS):
    #today = date.today()
    today = "2025-10-31"
    prev_from = "2025-10-27"
    losers = []
    total = len(symbols)
   
    for i, row in enumerate(symbols.itertuples(), 1):
        
        #progress(i, total)
        sym = row.fyers_symbol
      
        try:
            # get 5-min intraday for today
                   
            intraday_df = fetch_history(fyers, sym, "5", today, today)

            first_candle = intraday_df[intraday_df["time"] == datetime.strptime("09:15", "%H:%M").time()]
            if first_candle.empty:
                continue
            
            first_close = float(first_candle.iloc[0]['close'])
            # print("first_close", first_close)
            # previous day close: fetch daily for prev two days to be safe
            # prev_from = today - timedelta(days=5).strftime("%Y-%m-%d")
           
            prev_df = fetch_history(fyers, sym, "D", prev_from, today)
            #print("prev_df", prev_df)
            prev_close = float(prev_df.iloc[-2]['close'])  # previous trading day close
            # print("prev_close", prev_close)
            change_pct = ((first_close - prev_close) / prev_close) * 100
            #print ("change_pct", change_pct)
            losers.append({"symbol": sym, "change_pct": change_pct, "prev_close": prev_close})
            time.sleep(0.02)
        except Exception:
            continue

    df_losers = pd.DataFrame(losers)
    #print("df_losers",df_losers)
    if df_losers.empty:
        return []
    df_losers = df_losers.sort_values(by="change_pct").head(n)
    
    return df_losers

# ---------- Step 2: analyze each loser with Camarilla and extra checks ----------
def analyze_and_build_rows(fyers, df_losers, symbols_df):
    out_rows = []
    #today = date.today().strftime("%Y-%m-%d")
    today = "2025-10-31"
    dstart = "2025-10-27"
    total = len(df_losers)
    for i, r in enumerate(df_losers.itertuples(), 1):
        #progress(i, total)
        sym = r.symbol
        try:
            # daily history for previous day OHLC and weekly context
            #dstart = today - timedelta(days=20).strftime("%Y-%m-%d")
            ddf = fetch_history(fyers, sym, "D", dstart, today)
            if len(ddf) < 2:
                continue

            prev_row = ddf.iloc[-2] 
            prev_open = float(prev_row['open']); prev_high = float(prev_row['high'])
            prev_low = float(prev_row['low']); prev_close = float(prev_row['close'])

            # Yesterday faller check
            if not (prev_close < prev_open):
                continue

            # Weekly context: simple weekly pivot check
            # weekly_df = ddf.tail(10)
            # weekly_high = weekly_df['high'].astype(float).max()
            # weekly_low = weekly_df['low'].astype(float).min()
            # prev_week_close = float(weekly_df.iloc[-6]['close'])
            # curr_week_close = float(weekly_df.iloc[-1]['close'])
            # weekly_trend_down = curr_week_close < prev_week_close

            cams = calculate_camarilla_levels(prev_high, prev_low, prev_close)
            # Pivot above support: pivot = (H+L+C)/3 of previous day
            pivot = (prev_high + prev_low + prev_close) / 3.0
            # if not weekly_trend_down:
            #     continue
            # require pivot above L1 (pivot > support)
            if not (pivot > cams["L1"]):
                continue
            # require camarilla inside weekly range
            #if cams["H5"] > weekly_high or cams["L5"] < weekly_low:
            #    continue

            # intraday 5-min to get first candle
            
            #intr = fetch_history(fyers, sym, "5", "2025-10-31", "2025-10-31")
            intraday_df = fetch_history(fyers, sym, "5", today, today)

            first_candle = intraday_df[intraday_df["time"] == datetime.strptime("09:15", "%H:%M").time()]
            print("first_candle 2", first_candle)
            if first_candle.empty:
                continue

            frow = first_candle.iloc[0]
            f_low = float(frow['low']); f_close = float(frow['close'])

            # Condition: first 5-min breaks previous day low AND breaks a Camarilla support
            broke_prev_low = f_low < prev_low
            broken_level = None
            sell_price = None
            # Check L1/L2/L3 in that order (L1 is the nearest support)
            if f_low < cams["L1"]:
                broken_level = "L1"; sell_price = cams["L1"]
            if f_low < cams["L2"]:
                broken_level = "L2"; sell_price = cams["L2"]
            if f_low < cams["L3"]:
                broken_level = "L3"; sell_price = cams["L3"]

            if not (broke_prev_low and broken_level is not None):
                continue

            # Determine targets & SLs
            target_s1 = cams["L4"]
            target_s2 = cams["L5"]
            sl_r1 = cams["H1"]
            sl_r2 = cams["H2"]

            # Approx qty will be entered as a formula in the sheet; compute an approximate integer for local use if needed
            # We'll push the formula into the sheet cell so it updates if user edits Sell Price.
            date_str = today.strftime("%d %b")

            # Lookup stock name from symbols_df (if present)
            name_row = symbols_df[symbols_df['fyers_symbol'] == sym]
            stock_name = name_row['Company Name'].iloc[0] if not name_row.empty else sym

            out_rows.append({
                "Date": date_str,
                "Stock Name": stock_name,
                "Sell Price": round(sell_price, 2),
                "SL R1": round(sl_r1, 2),
                "SL R2": round(sl_r2, 2),
                "Target S4": round(target_s1, 2),
                "Target S5": round(target_s2, 2),
                "Broken Level": broken_level,
                "Change% (first5 vs prev_close)": round(r.change_pct, 2)
            })

            time.sleep(0.02)
        except Exception:
            continue
    return out_rows

# ---------- Write results to Google Sheet ----------
def write_to_sheet(rows):
    if not rows:
        print("No rows to write.")
        return

    gc = gsheets_client()
    try:
        sh = gc.open(SHEET_NAME)
    except gspread.SpreadsheetNotFound:
        sh = gc.create(SHEET_NAME)
        # share is required if the sheet owner isn't the service account (but since shared earlier, usually not needed)
    ws = sh.sheet1

    # Header layout (exact columns you requested)
    headers = ["Date", "Stock Name", "Approx Qty", "Sell Price", "SL R1/R2", "Target S4/s5"]
    ws.clear()
    ws.append_row(headers)

    # Append rows; we will set Approx Qty as formula referencing Sell Price cell
    for idx, r in enumerate(rows, start=2):  # start=2 because header on row 1
        sell = r["Sell Price"]
        sl_r1 = r["SL R1"]
        sl_r2 = r["SL R2"]
        target1 = r["Target S4"]
        target2 = r["Target S5"]
        # Compose SL and Target fields as required strings
        sl_field = f"{sl_r1}/{sl_r2}"
        target_field = f"{target1}/{target2}"

        # Write row values except Approx Qty (we'll set formula)
        ws.update(f"A{idx}:F{idx}", [[r["Date"], r["Stock Name"], "", sell, sl_field, target_field]])

        # Set Approx Qty cell formula: =IFERROR((100000 / D{row}) * 4.76, "")
        qty_formula = f"=IFERROR((100000 / D{idx}) * 4.76, \"\")"
        ws.update_acell(f"C{idx}", qty_formula)

    # Make sheet editable by owner (the spreadsheet is shared to the service account already).
    print(f"Wrote {len(rows)} rows to sheet '{SHEET_NAME}'.")

# ---------- MAIN ----------
def main(fyers):
    
    symbols_df = load_symbols()
    print(f"🔎 Loaded {len(symbols_df)} Nifty 500 symbols.")
   
    # 1) compute top 50 losers after first 5-min candle (should run at ~09:20)
    df_losers = get_top_n_losers_first5(fyers, symbols_df)
    print ("df_losers", df_losers)
    if not isinstance(df_losers, pd.DataFrame) or df_losers.empty:
        print("No losers found.")
        return

    top_symbols = df_losers
    print("top_symbols",top_symbols)
    # 2) analyze these symbols and build rows
    rows = analyze_and_build_rows(fyers, top_symbols, symbols_df)
    
    if not rows:
        print("No candidates passed all checks.")
        return

    # 3) write to google sheet
    write_to_sheet(rows)

# ---------- End script ----------
# To run: initialize your fyers client and call main(fyers)
# Example:
# from fyers_apiv3 import fyersModel
# fyers = fyersModel.FyersModel(client_id=..., token=...)
main(fyers)