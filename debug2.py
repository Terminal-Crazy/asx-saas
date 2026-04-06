import yfinance as yf

raw = yf.download(
    tickers=["BHP.AX", "CBA.AX"],
    period="1d",
    interval="1d",
    group_by="ticker",
    auto_adjust=True,
    progress=False
)

print(raw)
print("---")
print(raw.columns.tolist())