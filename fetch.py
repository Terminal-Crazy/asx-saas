import requests
import yfinance as yf
import pandas as pd
from io import StringIO
import os
import time

# Pull the ASX listed companies CSV, check HTTP request
print("Fetching ASX ticker list...")
asx_url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
response = requests.get(asx_url)
response.raise_for_status()

# Parses CSV, creates list array from 3-letter stock code
asx_df = pd.read_csv(StringIO(response.text), skiprows=2, header=None)
tickers = [t for t in asx_df[1].dropna().tolist() if t != "ASX code"]
yf_tickers = [f"{t}.AX" for t in tickers]
print(f"Found {len(yf_tickers)} tickers.")

# Break into chunks to avoid rate-limiting from Yahoo API
CHUNK_SIZE = 50
PAUSE_SECONDS = 7

chunks = [yf_tickers[i:i+CHUNK_SIZE] for i in range(0, len(yf_tickers), CHUNK_SIZE)]
total_chunks = len(chunks)
print(f"Downloading in {total_chunks} chunks of {CHUNK_SIZE}.")

all_rows = []
needs_fallback = []

for i, chunk in enumerate(chunks):
    print(f"  Chunk {i+1}/{total_chunks}...", end=" ")
    start = time.time()

    try:
        raw = yf.download(
            tickers=chunk,
            period="5d",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False
        )

        for ticker in chunk:
            try:
                series = raw[(ticker, "Close")].dropna()
                volume_series = raw[(ticker, "Volume")].dropna()

                if len(series) == 0:
                    raise ValueError("No data in 5d window")

                latest_close = series.iloc[-1]
                latest_volume = volume_series.iloc[-1] if len(volume_series) > 0 else 0

                if pd.isna(latest_close):
                    raise ValueError("NaN price")

                if latest_volume == 0 and len(series) >= 2:
                    price = round(float(series.iloc[-2]), 3)
                    last_traded = series.index[-2].date()
                    source = "prev_close"
                else:
                    price = round(float(latest_close), 3)
                    last_traded = series.index[-1].date()
                    source = "close"

                all_rows.append({
                    "ticker": ticker,
                    "price": price,
                    "last_traded": last_traded,
                    "source": source
                })

            except Exception:
                # Flag for fallback lookup
                needs_fallback.append(ticker)

    except Exception as e:
        print(f"Chunk failed: {e}")
        for ticker in chunk:
            needs_fallback.append(ticker)

    elapsed = round(time.time() - start, 1)
    print(f"done in {elapsed}s")

    if i < total_chunks - 1:
        time.sleep(PAUSE_SECONDS)

# fallback run: look back 1 year for last traded price
if needs_fallback:
    print(f"\nRunning fallback lookback for {len(needs_fallback)} tickers...")
    fb_chunks = [needs_fallback[i:i+CHUNK_SIZE] for i in range(0, len(needs_fallback), CHUNK_SIZE)]
    total_fb = len(fb_chunks)

    for i, chunk in enumerate(fb_chunks):
        print(f" Fallback chunk {i+1}/{total_fb}...", end=" ", flush=True)
        start = time.time()

        try:
            raw = yf.download(
                tickers=chunk,
                period="1y",
                interval="1d",
                group_by="ticker",
                auto_adjust=True,
                progress=False
            )

            for ticker in chunk:
                try:
                    series = raw[(ticker, "Close")].dropna()

                    if len(series) == 0:
                        raise ValueError("No data in 1y window")

                    last_price = round(float(series.iloc[-1]), 3)
                    last_traded = series.index[-1].date()

                    all_rows.append({
                        "ticker": ticker,
                        "price": last_price,
                        "last_traded": last_traded,
                        "source": "last_traded"
                    })

                except Exception:
                    all_rows.append({
                        "ticker": ticker,
                        "price": None,
                        "last_traded": None,
                        "source": "no_data"
                    })

        except Exception as e:
            print(f"Fallback chunk failed: {e}")
            for ticker in chunk:
                all_rows.append({
                    "ticker": ticker,
                    "price": None,
                    "last_traded": None,
                    "source": "no_data"
                })

        elapsed = round(time.time() - start, 1)
        print(f"done in {elapsed}s")

        if i < total_fb - 1:
            time.sleep(PAUSE_SECONDS)

# Output to CSV - needs to be modified to go to Network Share
os.makedirs("output", exist_ok=True)
out = pd.DataFrame(all_rows)
out = out.sort_values("ticker").reset_index(drop=True)
out.to_csv("output/prices.csv", index=False)

close_count = (out["source"] == "close").sum()
prev_count = (out["source"] == "prev_close").sum()
last_count = (out["source"] == "last_traded").sum()
no_data_count = (out["source"] == "no_data").sum()

print(f"\nDone.")
print(f" {close_count} prices from today's close")
print(f" {prev_count} prices from previous close (zero volume today)")
print(f" {last_count} prices from last trade within 1 year")
print(f" {no_data_count} had no data")

if no_data_count > 0:
    no_data = out[out["source"] == "no_data"][["ticker"]]
    no_data.to_csv("output/no_data.csv", index=False)
    print(f" Truly delisted tickers saved to output/no_data.csv")
