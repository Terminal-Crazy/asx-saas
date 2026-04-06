import requests
import pandas as pd
from io import StringIO

response = requests.get("https://www.asx.com.au/asx/research/ASXListedCompanies.csv")
asx_df = pd.read_csv(StringIO(response.text), skiprows=3)
print(asx_df.columns.tolist())
print(asx_df.head(3))