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
CHUNK_SIZE = 200
PAUSE_SECONDS = 5

chunks = [yf_tickers[i:i+CHUNK_SIZE] for i in range(0, len(yf_tickers), CHUNK_SIZE)]
total_chunks = len(chunks)
print(f"Downloading in {total_chunks} chunks of {CHUNK_SIZE}.")

all_rows = []
failed = []

for i, chunk in enumerate(chunks):
    print(f"  Chunk {i+1}/{total_chunks}...", end=" ")
    start = time.time()

    try:
        raw = yf.download(
            tickers=chunk,
            period="1d",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False
        )

        for ticker in chunk:
            try:
                close = raw[(ticker, "Close")].iloc[-1]
                if pd.isna(close):
                    raise ValueError("NaN price")
                all_rows.append({"ticker": ticker, "price": round(float(close), 3)})
            except Exception:
                all_rows.append({"ticker": ticker, "price": None})
                failed.append(ticker)

    except Exception as e:
        print(f"Chunk failed: {e}")
        for ticker in chunk:
            all_rows.append({"ticker": ticker, "price": None})
            failed.append(ticker)

    elapsed = round(time.time() - start, 1)
    print(f"done in {elapsed}s")

    if i < total_chunks - 1:
        time.sleep(PAUSE_SECONDS)

# Output to CSV - needs to be modified to go to Network Share
os.makedirs("output", exist_ok=True)
out = pd.DataFrame(all_rows)
out.to_csv("output/prices.csv", index=False)
print(f"\nDone. {out['price'].notna().sum()} prices saved, {len(failed)} had no data.")

if failed:
    pd.DataFrame({"ticker": failed}).to_csv("output/no_data.csv", index=False)
    print(f"Tickers with no data saved to output/no_data.csv")
